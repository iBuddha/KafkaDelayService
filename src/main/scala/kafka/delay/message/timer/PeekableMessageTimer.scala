package kafka.delay.message.timer

import java.util.concurrent.{BlockingQueue, DelayQueue, TimeUnit}
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.delay.message.timer.PeekableTimingWheel.Peeked
import kafka.delay.message.timer.bucket.TimerBucket
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.SystemTime
import kafka.utils.threadsafe

import scala.collection.SortedSet

@threadsafe
class PeekableMessageTimer(val outQueue: BlockingQueue[DelayMessageMeta],
                           val tickMs: Long = 1000,
                         wheelSize: Int = 60,
                         startMs: Long = SystemTime.hiResClockMs) extends MessageTimer {

  private[this] val delayQueue = new DelayQueue[TimerBucket]()
  private[this] val messageCounter = new AtomicLong(0)
  private[this] val timingWheel = new PeekableTimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = messageCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(delayMessage: DelayMessageMeta): Unit = {
    readLock.lock()
    try {
      addMessage(delayMessage)
    } finally {
      readLock.unlock()
    }
  }

  def add(delayMessages: Traversable[DelayMessageMeta]): Unit = {
    readLock.lock()
    try {
      for (message <- delayMessages)
        addMessage(message)
    } finally {
      readLock.unlock()
    }
  }

  @inline
  private def addMessage(messageMeta: DelayMessageMeta): Unit = {
    if (!timingWheel.add(messageMeta)) {
      // Already expired or cancelled
      outQueue.put(messageMeta)
    }
  }

  private[this] val reinsert = (messageMeta: DelayMessageMeta) => addMessage(messageMeta)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          timingWheel.advanceClock(bucket.getExpiration())
          bucket.flush(reinsert)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }


//  def peek(count: Int, minExpireTimeAbs: Long):  Peeked  = {
//    readLock.lock()
//    try {
//      timingWheel.peek(count, minExpireTimeAbs)
//    }finally {
//      readLock.unlock()
//    }
//  }

  /**
    *
    * @param count 当minBucketNum内的数量小于count时，会接下来再peek数个bucket
    * @param maxExpireTimeAbs exclude
    * @param minExpireTimeAbs  include
    * @param miniBucketNum 至少包括几个bucket的内容。以保证能peek到足够长的时间
    * @return
    */
  def peek(count: Int, maxExpireTimeAbs: Long, minExpireTimeAbs: Long, miniBucketNum: Int, latestPeeked: Map[Long, SortedSet[DelayMessageMeta]]):  Peeked  = {
    readLock.lock()
    try {
      timingWheel.peek(count, maxExpireTimeAbs, minExpireTimeAbs, miniBucketNum, latestPeeked)
    }finally {
      readLock.unlock()
    }
  }

  def getCurrentTime: Long = {
    readLock.lock()
    try {
      timingWheel.getCurrentTime
    }finally {
      readLock.unlock()
    }
  }

  def size: Long = messageCounter.get

  override def shutdown() {
  }

}