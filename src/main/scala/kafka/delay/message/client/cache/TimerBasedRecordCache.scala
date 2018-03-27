package kafka.delay.message.client.cache

import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.client.MessageConsumer
import kafka.delay.message.client.cache.RecordCache.{CacheElement, CachedAndNot}
import kafka.delay.message.client.cache.RecordWheelCache.Bucket
import kafka.delay.message.client.parser.RecordExpireTimeParser
import kafka.delay.message.timer.meta.{ArrayOffsetBatch, OffsetBatch}
import kafka.delay.message.utils.{SystemTime, Utils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ListBuffer}

class TimerBasedRecordCache(private var maxBytes: Int,
                            maxExpireMs: Long,
                            paddingTime: Long,
                            expireMsParser: RecordExpireTimeParser) extends RecordCache {

  import TimerBasedRecordCache._

  //  require(maxBytes > 0)

  private val wheel = new RecordWheelCache(TickMs, (maxExpireMs / TickMs).toInt, SystemTime.milliseconds)

  private var version: Long = 0L
  private var usedSize: Int = _

  private var savedByteRead: Long = 0

  private val cachedRecords: mutable.HashMap[Long, Element] = mutable.HashMap.empty
  //  private val permanentRemoved = new OffsetCache(1024 * 1024)

  override def getSizeInBytes(): Int = usedSize

  override def getVersion(): Long = version

  private def performance: Double = if (usedSize > 0) savedByteRead / usedSize.toDouble else 0

  override def age(): Unit = savedByteRead = (savedByteRead * 0.9).toLong

  def recordCount = cachedRecords.size

  private var latestExpiredMs = -1L

  /**
    *
    * @return 是否由于空间不够而不得不删除本该cache的消息
    */
  override def +=(record: ConsumerRecord[Array[Byte], Array[Byte]], now: Long): Boolean = {
    var notEnoughSpace = false
    if (!cachedRecords.contains(record.offset)) {
      val expireMs = expireMsParser.getExpireMs(record)
      if (canAdd(expireMs)) {
        val recordSize = RecordSizeSampler.bytes(record)
        notEnoughSpace = forceAdd(record, recordSize, expireMs, now)
      }
    } else {
      //      logger.debug("can't add {}", record.offset())
    }
    notEnoughSpace
  }

  def tryAdd(record: ConsumerRecord[Array[Byte], Array[Byte]], now: Long): Boolean = {
    if (!cachedRecords.contains(record.offset)) {
      val expireMs = expireMsParser.getExpireMs(record)
      if (canAdd(expireMs)) {
        val recordSize = RecordSizeSampler.bytes(record)
        tryAdd(record, recordSize, expireMs, now)
      } else
        false
    } else {
      false
    }
  }


  override def -=(offset: Long) = {
    cachedRecords.remove(offset).foreach { element: Element =>
      usedSize -= element.cacheElement.size
      element.bucket.records.remove(offset)
    }
    this
  }

  def containsAll(batch: OffsetBatch): Boolean = {
    batch.forall(offset => cachedRecords.contains(offset))
  }

  override def contains(offset: Long) = cachedRecords.contains(offset)

  /**
    *
    * @param batch
    */
  override def get(batch: OffsetBatch): CachedAndNot = {
    var latestContainedOffset = -1L
    val cached = new ListBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    val nonCached = new ArrayBuffer[Long](batch.size)
    batch.foreach { offset =>
      if (cachedRecords.contains(offset)) {
        val record = cachedRecords(offset)
        cached += record.cacheElement.record
        savedByteRead = savedByteRead + record.cacheElement.size
        latestContainedOffset = offset
        this -= offset
      } else
        nonCached += offset
    }
    if (cached.isEmpty) {
      CachedAndNot(List.empty, Some(batch))
    } else if (nonCached.isEmpty) {
      CachedAndNot(cached.toList, None)
    } else {
      val expireMs = expireMsParser.getExpireMs(cached.last)
      latestExpiredMs = expireMs
      CachedAndNot(cached.toList, Some(new ArrayOffsetBatch(nonCached.toArray)))
    }
  }

  override def test(batch: OffsetBatch): CachedAndNot = {
    val cached = new ListBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    val nonCached = new ArrayBuffer[Long](batch.size)
    batch.foreach { offset =>
      if (cachedRecords.contains(offset)) {
        val record = cachedRecords(offset)
        cached += record.cacheElement.record
        savedByteRead = savedByteRead + record.cacheElement.size
      } else
        nonCached += offset
    }
    if (cached.isEmpty) {
      CachedAndNot(List.empty, Some(batch))
    } else if (nonCached.isEmpty) {
      CachedAndNot(cached.toList, None)
    } else {
      CachedAndNot(cached.toList, Some(new ArrayOffsetBatch(nonCached.toArray)))
    }
  }


  /**
    * 同时会把已经在cache里的元素移除
    *
    * @param batch
    * @return
    */
  override def getCached(batch: OffsetBatch): List[CacheElement] = {
    val cached = mutable.ListBuffer.empty[CacheElement]
    batch.foreach { offset =>
      cachedRecords.get(offset).foreach { e =>
        cached += e.cacheElement
        savedByteRead += e.cacheElement.size
        this -= e.cacheElement.record.offset()
      }
    }
    cached.toList
  }

  def get(offset: Long): Option[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    cachedRecords.get(offset).map(_.cacheElement.record)
  }


  override def resize(newSize: Int, now: Long, version: Long): Unit = {
    this.version = version
    if (maxBytes != newSize) {
      require(newSize >= 0, "newSize: " + newSize)
      if (newSize < maxBytes) {
        trim(newSize, now - RecordCache.ResizePaddingMs)
      }
      maxBytes = newSize
    }
  }


  /**
    * 将cache大小缩减到指定的值
    *
    * @param targetSize
    */
  def trim(targetSize: Int, trimBefore: Long): Unit = {
    removeExpiredBefore(trimBefore)
    //TODO: 这里不应该判断removeFarthest的返回值。现在加上，是为了避免有bug使得此方法进入死循环
    while (usedSize > targetSize && removeFarthest() != 0) {}
  }


  /**
    * 强制加入这个记录到cache中，如果加入后cache过大，strip掉过期的消息，然后重试
    *
    * @return 是由于空间不够而被迫移除了record
    */
  private def forceAdd(record: ConsumerRecord[Array[Byte], Array[Byte]],
                       recordSize: Int,
                       expireMs: Long, now: Long): Boolean = {
    val needRemove = recordSize + usedSize > maxBytes
    var removedUnExpired = false
    if (needRemove) {
      removeExpiredBefore(now - RecordCache.ResizePaddingMs)
      if (recordSize + usedSize > maxBytes && latestExpiredMs != -1L) {
        removeExpiredBefore(latestExpiredMs)
      }
      if (recordSize + usedSize > maxBytes && expireMs <= wheel.maxMs) {
        var released = true
        var releasedSize = 0
        while (recordSize + usedSize > maxBytes && released) {
          val thisReleased = removeFarthest()
          released = thisReleased != 0 //TODO: 这里移除最晚超时的bucket并非最优选择
          releasedSize += thisReleased
          removedUnExpired = true
        }
        logger.debug("released {} not yet expired cached records", releasedSize)
      }
      if (recordSize + usedSize <= maxBytes) {
        add(record, recordSize, expireMs)
      } else {
        logger.debug("give up add record with offset {}", record.offset())
      }
    } else {
      add(record, recordSize, expireMs)
    }
    if (removedUnExpired || usedSize > maxBytes * RecordCache.ScaleUpRatio) //提前申请内存
      true
    else
      false
  }


  private def tryAdd(record: ConsumerRecord[Array[Byte], Array[Byte]],
                     recordSize: Int,
                     expireMs: Long, now: Long): Boolean = {
    if (recordSize + usedSize > maxBytes) {
      removeExpiredBefore(now - RecordCache.ResizePaddingMs)
    }
    if (recordSize + usedSize <= maxBytes) {
      add(record, recordSize, expireMs)
    }
    if (usedSize > maxBytes * RecordCache.ScaleUpRatio) //提前申请内存
      true
    else
      false
  }

  @inline
  private def add(record: ConsumerRecord[Array[Byte], Array[Byte]], recordSize: Int, expireMs: Long): Unit = {
    val element = wheel.put(record, recordSize, expireMs)
    if (element.isDefined) {
      cachedRecords.put(record.offset(), element.get)
      usedSize += recordSize
    } else {
      logger.debug("can't add {}", record.offset())
    }
  }

  @inline
  private def canAdd(expireMs: Long): Boolean = {
    //    if(isExpired(expireMs, now)){
    //      logger.debug("{} expired, now {}", expireMs)
    //    }
    !isExpired(expireMs) && expireMs <= wheel.minMs + maxExpireMs
  }

  //  /**
  //    * 移除已过期的元素
  //    *
  //    * @return 被移除的record总的大小
  //    */
  //  private def removeExpired(now: Long): Int = {
  //    var removedSize: Int = 0
  //    if (!cachedRecords.isEmpty && isExpired(cachedRecords.head._2.expireMs, now)) {
  //      val toRemove = cachedRecords.filter(_._2.expireMs + paddingTime <= now)
  //      toRemove.foreach {
  //        case (offset, _) =>
  //          removedSize += rm(offset, true)
  //      }
  //    }
  //    removedSize
  //  }

  /**
    * 移除过期时间在指定时间之前及它所在的bucket的所有元素。用于清除过期元素
    *
    * @param removeExpireBeforeMs
    * @return 被移除的record总的大小
    */
  override def removeExpiredBefore(removeExpireBeforeMs: Long): Unit = {
    val released = wheel.advanceAndClearExpired(removeExpireBeforeMs)
    released.foreach {
      case (offset, element) =>
        cachedRecords.remove(offset)
        usedSize -= element.cacheElement.size
    }
    logger.debug("remove expired before {}, count {}", Utils.milliToString(removeExpireBeforeMs), released.size)
  }

//  /**
//    * 移除过期时间在指定时间之后的元素
//    *
//    * @param base
//    * @return 被移除的record总的大小
//    */
//  private def removeExpiredAfter(base: Long): Unit = {
//    val released = wheel.expireAfter(base)
//    released.foreach {
//      case (offset, element) =>
//        cachedRecords.remove(offset)
//        usedSize -= element.cacheElement.size
//    }
//  }


  //  /**
  //    * 移除最早添加的元素
  //    *
  //    * @return 是否移除了元素
  //    */
  //  private def removeOldest(): Boolean = {
  //    if (cachedRecords.isEmpty)
  //      false
  //    else {
  //      rm(cachedRecords.head._1, false)
  //      true
  //    }
  //  }

  /**
    *
    * @return 是否真正释放了元素
    */
  private def removeFarthest(): Int = {
    var releasedSize = 0
    val released = wheel.releaseOneBucket()
    released.foreach {
      case (offset, element) =>
        releasedSize = releasedSize + 1
        cachedRecords.remove(offset)
        usedSize -= element.cacheElement.size
    }
    logger.debug("released {} un-expired cached records", releasedSize)
    releasedSize
  }

  /**
    * 如果超时时间 -  paddingTime == now，也认为是超时了
    */
  @inline
  private def isExpired(expireMs: Long): Boolean = {
    expireMs - paddingTime <= wheel.minMs
  }

  override def metrics(): RecordCacheMetrics = {
    if (latestExpiredMs != -1L) {
      removeExpiredBefore(latestExpiredMs - RecordCache.ResizePaddingMs)
    }
    age()
    RecordCacheMetrics(performance, maxBytes, usedSize, version)
  }

  override def getNonCachedOffsets(batch: OffsetBatch) = {
    val buffer = new mutable.ArrayBuffer[Long](batch.size)
    batch.foreach { offset =>
      if (!cachedRecords.contains(offset)) {
        buffer += offset
      }
    }
    new ArrayOffsetBatch(buffer.toArray)
  }
}

object TimerBasedRecordCache {
  val TickMs = 1000L
  val logger = LoggerFactory.getLogger(TimerBasedRecordCache.getClass)

  case class Element(cacheElement: CacheElement, bucket: Bucket)

}
