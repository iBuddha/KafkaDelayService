package kafka.delay.message.timer

import java.util.concurrent.DelayQueue
import java.util.concurrent.atomic.AtomicLong

import kafka.delay.message.timer.PeekableTimingWheel.{Peeked, PeekedBucket}
import kafka.delay.message.timer.bucket.{TimerBucket, VectorBasedTimerBucket}
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.Time
import kafka.utils.nonthreadsafe
import org.slf4j.LoggerFactory

import scala.collection.{SortedSet, mutable}
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

/**
  * 可以主动获取下一个bucket的内容
  */
@nonthreadsafe
private[delay] class PeekableTimingWheel(tickMs: Long, wheelSize: Int, startMs: Long, taskCounter: AtomicLong, delayQueue: DelayQueue[TimerBucket]) {
  require(wheelSize >= 2)
  private[this] val interval = tickMs * wheelSize
  private val buckets = Array.tabulate[TimerBucket](wheelSize) { _ => new VectorBasedTimerBucket(taskCounter) }

  private[this] var currentTime = startMs - (startMs % tickMs) // rounding down to multiple of tickMs
  private val overflowPeekDistance = Math.min(
    wheelSize - 1,
    PeekableTimingWheel.getOverflowWheelPeekDistance(tickMs))
  // overflowWheel can potentially be updated and read by two concurrent threads through add().
  // Therefore, it needs to            be volatile due to the issue of Double-Checked Locking pattern with JVM
  @volatile private[this] var overflowWheel: PeekableTimingWheel = null

  private[this] def addOverflowWheel(): Unit = {
    synchronized {
      if (overflowWheel == null) {
        overflowWheel = new PeekableTimingWheel(
          tickMs = interval,
          wheelSize = wheelSize,
          startMs = currentTime,
          taskCounter = taskCounter,
          delayQueue
        )
      }
    }
  }

  def getCurrentTime: Long = currentTime

  private def nextBucket(): TimerBucket = {
    buckets(((currentTime / tickMs + 1) % wheelSize.toLong).toInt)
  }


  //TODO: 正确的逻辑要比这复杂。因为即使在当前的wheel内可以peek到足够的元素，但是时间轮转运时，可能有overflow wheel里的元素注入到接下来的当前wheel中。
  /**
    * 所以，接下来会到期的元素不仅可能在当前wheel中，也可能在overflow wheel中。
    * 因此，只peek当前wheel的元素(即使当前wheel内有足够多的元素)仍不足以覆盖所有可能到期的元素。
    * 但是，由于overflow wheel的tickMs很大，而且只在当前wheel的特定轮数才可能触发overflow wheel。所以，这种未能peek的元素只会周期性地
    * 出现。而且只有第一个当前wheel的tickMs的时间段内的元素可能会被漏掉。从而造成cache系统未能预读这部分元素对应的message。
    * 因此，当有cache未命中时，并不能确定cache系统本身有问题。
    */

  private def peekOverflowWheel(maxNumber: Int, endTimeAbs: Long, beginTimeAbs: Long, buffer: ListBuffer[DelayMessageMeta]) = {
    if (overflowWheel != null) {
      val latestOverflowBucket = overflowWheel.nextBucket()
      var completed = false
      var count = 0
      //TODO: 这里可能需要按照下一层wheel的tickMs，会批次的filter。比如，先加入第一个tickMs的消息，然后是第二个
      if (latestOverflowBucket.getExpiration() == beginTimeAbs) {
        latestOverflowBucket.foreach { meta =>
          if (meta.offset != -1 && meta.expirationMsAbsolutely <= endTimeAbs && meta.expirationMsAbsolutely >= beginTimeAbs && !completed) {
            buffer += meta
            count = count + 1
            if (count >= maxNumber) {
              completed = true
            }
          }
        }
      }
      PeekableTimingWheel.logger.debug("peeked {} from overflow wheel", count)
    }
    buffer.toList
  }

  /**
    * 小于miniBucketNum的bucket的所有内容，以及不超过endTimeAbs的bucket内的，使得总计数量不超地maxNumber的bucket内的所有消息都会被加
    * 入结果集。
    * 注意：一个bucket的元素，要不被加入结果，要不不被加入结果
    *
    * @param maxNumber
    * @param endTimeAbs   exclude
    * @param beginTimeAbs include
    * @param miniBucketNum
    * @param lastPeekedBuckets        key为bucketTime, value为这个bucket的已知元素
    * @return
    */
  def peek(maxNumber: Int,
           endTimeAbs: Long,
           beginTimeAbs: Long,
           miniBucketNum: Int,
           lastPeekedBuckets: Map[Long, SortedSet[DelayMessageMeta]]): Peeked = {
    val startTime = Math.max(beginTimeAbs - tickMs, currentTime)
    var nextBucketId = startTime / tickMs + 1
    val maxBucketId = nextBucketId + wheelSize - 2
    val bucketBuffer = new ListBuffer[PeekedBucket]
    var bucket: TimerBucket = buckets((nextBucketId % wheelSize.toLong).toInt)
    var currentBucketTime = bucket.getExpiration()
    var complete = if (bucket.getExpiration() > currentTime) false else true
    var bucketCount = 0
    val overflowed = new ListBuffer[DelayMessageMeta]
    var messageCount = 0
    if (!complete) {
      //如果只读取与下一个bucket同时到期的overflow wheel，cache系统可能会来不及读取。所以再提前一些
      //TODO: 当delay time非常离散，roll到的batch里的消息数量非常少时，这里peek到的元素的数量可能会太少。
      //使得在overflow wheel里的下一个bucket到期时，cache系统会需要处理特别多的消息.
      //需要使得这个过渡的过程更加平滑。即，每次位于下一层wheel的peek都多peek到一点上层wheel里的消息
      if (overflowWheel != null &&
        currentBucketTime + tickMs * overflowPeekDistance == overflowWheel.nextBucket.getExpiration) {
        peekOverflowWheel(maxNumber,
          currentBucketTime + tickMs * (overflowPeekDistance + 5),
          currentBucketTime + tickMs * overflowPeekDistance,
          overflowed)
      }
    }
    messageCount += overflowed.size
    while (!complete) {
      val messageBuffer = mutable.SortedSet.empty[DelayMessageMeta]
      val lastPeeked = lastPeekedBuckets.get(currentBucketTime).getOrElse(SortedSet.empty[DelayMessageMeta])
      val peekedBucket =
      if  (lastPeeked.size != bucket.size()) {
        bucket.foreach { meta =>
          if (meta.offset != -1L) { //exclude mocked message
            messageBuffer += meta
            messageCount += 1
          }
        }
       PeekedBucket(currentBucketTime, messageBuffer)
      } else {
        messageBuffer ++=  lastPeeked
        messageCount += lastPeeked.size
        PeekedBucket(currentBucketTime, lastPeeked)
      }
      bucketBuffer += peekedBucket
      bucketCount = bucketCount + 1
      nextBucketId = nextBucketId + 1
      bucket = buckets((nextBucketId % wheelSize.toLong).toInt)
      currentBucketTime = bucket.getExpiration()
      if(messageCount >= maxNumber){
        complete = true
      }
      if(bucketCount < miniBucketNum){
        complete = false
      }
      if (bucket.getExpiration() >= endTimeAbs || bucket.getExpiration() <= currentTime) {
        complete = true
      }
      if (nextBucketId > maxBucketId) {
        complete = true
      }

    }
    Peeked(bucketBuffer.toList, overflowed.toList)
  }

  def add(delayedMessage: DelayMessageMeta): Boolean = {
    val expiration = delayedMessage.expirationMsAbsolutely

    if (expiration < currentTime + tickMs) {
      // Already expired
      false
    } else if (expiration < currentTime + interval) {
      // Put in its own bucket
      val virtualId = expiration / tickMs
      val bucket = buckets((virtualId % wheelSize.toLong).toInt)
      bucket.add(delayedMessage)

      // Set the bucket expiration time
      if (bucket.setExpiration(virtualId * tickMs)) {
        // The bucket needs to be enqueued because it was an expired bucket
        // We only need to enqueue the bucket when its expiration time has changed, i.e. the wheel has advanced
        // and the previous buckets gets reused; further calls to set the expiration within the same wheel cycle
        // will pass in the same value and hence return false, thus the bucket with the same expiration will not
        // be enqueued multiple times.
        //        logger.debug(s"add $bucket to DelayQueue")
        delayQueue.offer(bucket)
      }
      true
    } else {
      // Out of the interval. Put it into the parent timer
      if (overflowWheel == null) addOverflowWheel()
      overflowWheel.add(delayedMessage)
    }
  }

  // Try to advance the clock
  def advanceClock(timeMs: Long): Unit = {
    if (timeMs >= currentTime + tickMs) {
      currentTime = timeMs - (timeMs % tickMs)

      // Try to advance the clock of the overflow wheel if present
      if (overflowWheel != null) overflowWheel.advanceClock(currentTime)
    }
  }
}

object PeekableTimingWheel {

  val logger = LoggerFactory.getLogger(classOf[PeekableTimingWheel])

  case class Peeked(buckets: List[PeekedBucket], overflowed: List[DelayMessageMeta])

  case class PeekedBucket(bucketTime: Long, messages: SortedSet[DelayMessageMeta])

  private val OverflowWheelPeekDistance = 2048

  def getOverflowWheelPeekDistance(tickMs: Long) = OverflowWheelPeekDistance / tickMs
}