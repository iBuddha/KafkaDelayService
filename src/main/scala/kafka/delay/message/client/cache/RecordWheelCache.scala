package kafka.delay.message.client.cache

import kafka.delay.message.client.cache.RecordCache.CacheElement
import kafka.delay.message.client.cache.RecordWheelCache.Bucket
import kafka.delay.message.client.cache.TimerBasedRecordCache.Element
import kafka.delay.message.utils.Utils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 以类似timing wheel的形式存储cache里的记录。当需要驱逐元素时，会驱逐它所在的整个bucket。
  *
  */
class RecordWheelCache(tickMs: Long, wheelSize: Int, startMs: Long) {
  private[this] val buckets =
    Array.tabulate[Bucket](wheelSize)(_ => new Bucket(-1, mutable.Map.empty))

  private var currentTime = startMs - (startMs % tickMs) // include
  private val intervalMs = tickMs * wheelSize

  //可以被加到这个wheel的元素为[minMs, maxMs]

  /**
    * 此wheel里的最小时间，include
    */
  def minMs = currentTime

  /**
    * 此wheel里的最大时间，include
    *
    * @return
    */
  def maxMs = currentTime + intervalMs - 1

  /**
    * @return 加到了哪个bucket里
    */
  def put(record: ConsumerRecord[Array[Byte], Array[Byte]], recordSize: Int, expireMs: Long): Option[Element] = {
    if (expireMs >= currentTime && expireMs <= maxMs) {
      val virtualId = expireMs / tickMs
      val bucket = buckets((virtualId % wheelSize).toInt)
      if (!bucket.isEmpty && bucket.id != virtualId) {
        RecordWheelCache.logger.debug("illegal state, bucket id is {}, it should be {}", bucket.id, virtualId)
      }
      if(bucket.isEmpty){
        bucket.id = virtualId
      }
      val element = new Element(CacheElement(record, recordSize, expireMs), bucket)
      bucket += (record.offset, element)
      Some(element)
    } else
      None
  }

  /**
    * 将小于此时间的所有bucket过期。
    * 同时设置当前时间为指定时间
    */
  def advanceAndClearExpired(clearBeforeMs: Long): Map[Long, Element] = {
    if (clearBeforeMs < currentTime + tickMs) {
      RecordWheelCache.logger.debug("clearBeforeMs {} < current time {} + tickMs, cleared nothing",
        clearBeforeMs,
        currentTime)
      Map.empty[Long, Element]
    } else {
      val expired = mutable.Map.empty[Long, Element]
      val currentBucketId = currentTime / tickMs
      val expiredBucketId = clearBeforeMs / tickMs - 1
      (currentBucketId to expiredBucketId).foreach { bucketId =>
        val bucket = buckets((bucketId % wheelSize).toInt)
        if(!bucket.isEmpty ){
            expired ++= bucket.records
            bucket.clear()
        }
      }
      //      val expireBucketId = clearBeforeMs / tickMs
      //      buckets.foreach { bucket =>
      //        if(bucket.id < expireBucketId) {
      //          expired ++= bucket.records
      //          expired.clear()
      //        }
      //      }
      currentTime = (expiredBucketId + 1) * tickMs
      RecordWheelCache.logger.debug(s"cleared ${expired.size} records before {}, set current time to {}",
        Utils.milliToString(clearBeforeMs): Any,
        Utils.milliToString(currentTime): Any)
      expired.toMap
    }
  }

  //
  //  /**
  //    * 将大于等于此时间的所有bucket过期
  //    *
  //    * @param expireMs
  //    */
  //  def expireAfter(expireMs: Long): Map[Long, Element] = {
  //    if (expireMs > maxMs) {
  //      Map.empty[Long, Element]
  //    } else {
  //      val expired = mutable.Map.empty[Long, Element]
  //      val expireBucketId = expireMs / tickMs
  //      val tailBucketId = (currentTime + intervalMs - 1) / tickMs
  //      (expireBucketId to tailBucketId).foreach { id =>
  //        val expiredBucket = buckets((id % wheelSize).toInt)
  //        expired ++= expiredBucket
  //        expiredBucket.clear()
  //      }
  //      expired.toMap
  //    }
  //  }

  /**
    * 释放非空但是超时时间越晚的那个bucket
    *
    * @return
    */
  def releaseOneBucket(): Map[Long, Element] = {
    val tailBucketId = maxMs / tickMs
    val currentBucketId = currentTime / tickMs
    var i = tailBucketId
    var released = false
    var result = Map.empty[Long, Element]
    while (i >= currentBucketId && !released) {
      val bucket = buckets((i % wheelSize).toInt)
      if (!bucket.records.isEmpty) {
        released = true
        result = bucket.records.toMap
        bucket.records.clear()
      } else {
        i = i - 1
      }
    }
    RecordWheelCache.logger.debug("released {} records", result.size)
    result
  }
}

object RecordWheelCache {

  class Bucket(var id: Long, val records: mutable.Map[Long, Element]) {
    def clear(): Unit = {
      records.clear()
      id = -1L
    }

    def isEmpty = id == -1L

    def +=(offset: Long, element: Element) = {
      records.put(offset, element)
    }
  }

  val logger = LoggerFactory.getLogger(classOf[RecordWheelCache])
}
