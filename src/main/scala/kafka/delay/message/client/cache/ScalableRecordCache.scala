package kafka.delay.message.client.cache

import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.client.cache.RecordCache.{CacheElement, CachedAndNot}
import kafka.delay.message.client.parser.RecordExpireTimeParser
import kafka.delay.message.timer.meta.{ArrayOffsetBatch, OffsetBatch}
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * @param maxBytes
  */
class ScalableRecordCache(private var maxBytes: Int,
                          maxExpireMs: Long,
                          paddingTime: Long,
                          expireMsParser: RecordExpireTimeParser) extends RecordCache {

  require(maxBytes > 0)

  private var version: Long = 0L
  private var usedSize: Int = _

  private var savedByteRead: Long = 0

  private val cachedRecords: mutable.LinkedHashMap[Long, CacheElement] =
    mutable.LinkedHashMap.empty
  private val permanentRemoved = new OffsetCache(1024 * 1024)


  /**
    *
    * @return 是否由于空间不够而不得不删除本该cache的消息
    */
  override def +=(record: ConsumerRecord[Array[Byte], Array[Byte]], now: Long): Boolean = {
    var notEnoughSpace = false
    if (!permanentRemoved.contains(record.offset)) {
      if (!cachedRecords.contains(record.offset)) {
        val recordSize = RecordSizeSampler.bytes(record)
        val expireMs = expireMsParser.getExpireMs(record)
        if (canAdd(expireMs, now)) {
          notEnoughSpace = add(record, recordSize, expireMs, now)
        }
      }
    }
    notEnoughSpace
  }


  override def -=(offset: Long) = {
    rm(offset, true)
    this
  }

  def containsAll(batch: OffsetBatch): Boolean = {
    batch.forall(offset => cachedRecords.contains(offset))
  }

  override def contains(offset: Long) = cachedRecords.contains(offset)

  /**
    *
    * @param batch
    * @return 如果获取了全部的消息，Option[MetaBatch]为None.如果部分获取了，两个Option都不是None。如果完全不在cache里，第一个Option
    *         为None
    */
  override def get(batch: OffsetBatch): CachedAndNot = {
    var latestContainedOffset = -1L
    val cached = new ListBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    batch.find { offset =>
      if (cachedRecords.contains(offset)) {
        val cacheElement = cachedRecords(offset)
        cached += cacheElement.record
        savedByteRead = savedByteRead + cacheElement.size
        latestContainedOffset = offset
        false
      } else
        true
    }
    if (latestContainedOffset == -1) {
      CachedAndNot(List.empty, Some(batch))
    } else if (cached.size == batch.size) {
      CachedAndNot(cached.toList, None)
    } else {
      val trimedBatch = batch.from(latestContainedOffset + 1)
      CachedAndNot(cached.toList, trimedBatch)
    }
  }

  override def test(batch: OffsetBatch): CachedAndNot = {
    var latestContainedOffset = -1L
    val cached = new ListBuffer[ConsumerRecord[Array[Byte], Array[Byte]]]
    batch.find { offset =>
      if (cachedRecords.contains(offset)) {
        val cacheElement = cachedRecords(offset)
        cached += cacheElement.record
        savedByteRead = savedByteRead + cacheElement.size
        latestContainedOffset = offset
        false
      } else
        true
    }
    if (latestContainedOffset == -1) {
      CachedAndNot(List.empty, Some(batch))
    } else if (cached.size == batch.size) {
      CachedAndNot(cached.toList, None)
    } else {
      val trimedBatch = batch.from(latestContainedOffset + 1)
      CachedAndNot(cached.toList, trimedBatch)
    }
  }

  def get(offset: Long): Option[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    cachedRecords.get(offset).map(_.record)
  }

  override def recordCount = cachedRecords.size

  override def getSizeInBytes(): Int = usedSize

  override def getVersion(): Long = version

  private def performance: Double = if (usedSize > 0) savedByteRead / usedSize.toDouble else 0

  override def age(): Unit = savedByteRead = (savedByteRead * 0.9).toLong

  override def resize(newSize: Int, now: Long, version: Long): Unit = {
    this.version = version
    age()
    if (maxBytes != newSize) {
      require(newSize >= 0, "newSize: " + newSize)
      if (newSize < maxBytes) {
        trim(newSize, now)
      }
      maxBytes = newSize
    }
  }


  /**
    * 将cache大小缩减到指定的值
    *
    * @param targetSize
    */
  def trim(targetSize: Int, now: Long): Unit = {
    removeExpired(now)
    if (usedSize > targetSize) {
      val sortedByExpireMs = cachedRecords.iterator.map { e =>
        (e._1, e._2.expireMs)
      }.toArray.sortBy(_._2)
      val iter = sortedByExpireMs.iterator
      while (usedSize > targetSize && iter.hasNext) {
        val (offset, _) = iter.next()
        rm(offset, false)
      }
    }
  }

  /**
    * 移除已过期的元素
    *
    * @return 被移除的record总的大小
    */
  private def removeExpired(now: Long): Int = {
    var removedSize: Int = 0
    if (!cachedRecords.isEmpty && isExpired(cachedRecords.head._2.expireMs, now)) {
      val toRemove = cachedRecords.filter(_._2.expireMs + paddingTime <= now)
      toRemove.foreach {
        case (offset, _) =>
          removedSize += rm(offset, true)
      }
    }
    removedSize
  }


  /**
    * 移除最早添加的元素
    *
    * @return 是否移除了元素
    */
  private def removeOldest(): Boolean = {
    if (cachedRecords.isEmpty)
      false
    else {
      rm(cachedRecords.head._1, false)
      true
    }
  }


  /**
    * @param offset
    * @param permanentRemove
    * @return 被移除的元素的大小
    */
  private def rm(offset: Long, permanentRemove: Boolean): Int = {
    var removedSize = 0
    if (permanentRemove) {
      permanentRemoved.add(offset)
    }
    cachedRecords.remove(offset).foreach { element: CacheElement =>
      removedSize = RecordSizeSampler.bytes(element.record)
      usedSize -= removedSize
    }
    removedSize
  }

  /**
    * 强制加入这个记录到cache中，如果加入后cache过大，strip掉过期的消息，然后重试
    *
    * @return 是由于空间不够而被迫移除了record
    */
  private def add(record: ConsumerRecord[Array[Byte], Array[Byte]],
                  recordSize: Int,
                  expireMs: Long,
                  now: Long): Boolean = {
    if (recordSize > maxBytes) {
      true
    } else {
      if (recordSize + usedSize > maxBytes) {
        removeExpired(now)
      }
      val needRemove = recordSize + usedSize > maxBytes
      while (recordSize + usedSize > maxBytes) {
        removeOldest()
      }
      add(record, recordSize, expireMs)
      needRemove
    }
  }

  private def add(record: ConsumerRecord[Array[Byte], Array[Byte]], recordSize: Int, expireMs: Long): Unit = {
    cachedRecords.put(record.offset(), new CacheElement(record, recordSize, expireMs))
    usedSize += recordSize
  }

  private def canAdd(expireMs: Long, now: Long): Boolean = {
    !isExpired(expireMs, now) && expireMs <= now + maxExpireMs
  }

  /**
    * 如果超时时间 -  paddingTime == now，也认为是超时了
    */
  private def isExpired(expireMs: Long, now: Long): Boolean = {
    expireMs - paddingTime <= now
  }

  override def metrics(): RecordCacheMetrics = {
    removeExpired(System.currentTimeMillis())
    RecordCacheMetrics(performance, maxBytes, usedSize, version)
  }

  override def getCached(batch: OffsetBatch): List[CacheElement] = {
    val cached = mutable.ListBuffer.empty[CacheElement]
    batch.foreach { offset =>
      cachedRecords.get(offset).foreach(cached +=)
    }
    cached.toList
  }

  override def removeExpiredBefore(removeExpireMsBefore: Long): Unit = {
    cachedRecords.retain {
      case (_, element) =>
        element.expireMs >= removeExpireMsBefore
    }
  }

  override def getNonCachedOffsets(required: OffsetBatch) = {
    val buffer = new mutable.ArrayBuffer[Long](required.size)
    required.foreach { offset =>
      if(!cachedRecords.contains(offset)){
        buffer += offset
      }
    }
    new ArrayOffsetBatch(buffer.toArray)
  }
}

