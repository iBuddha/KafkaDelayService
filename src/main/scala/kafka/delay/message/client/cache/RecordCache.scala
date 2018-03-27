package kafka.delay.message.client.cache

import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.client.cache.RecordCache.{CacheElement, CachedAndNot}
import kafka.delay.message.timer.meta.OffsetBatch
import kafka.delay.message.utils.{DelayServiceConfig, Time}
import org.apache.kafka.clients.consumer.ConsumerRecord


trait RecordCache{
  /**
    * 会移除已经cache并且在batch里的元素
    */
  def get(batch: OffsetBatch): CachedAndNot

  /**
    * 检测是否在cache里，不会移除已在cache里的元素
    */
  def test(batch: OffsetBatch): CachedAndNot
  def getCached(batch: OffsetBatch): List[CacheElement]
  def getNonCachedOffsets(required: OffsetBatch): OffsetBatch
  def recordCount: Int
  def getSizeInBytes(): Int
  def getVersion(): Long
  def metrics(): RecordCacheMetrics
  def age(): Unit //使之前记录变老
  def removeExpiredBefore(removeBeforeMs: Long): Unit
  def contains(offset: Long): Boolean

  /**
    *
    * @param newSize
    * @param now clock time
    * @param version
    */
  def resize(newSize: Int, now: Long, version: Long): Unit


  /**
    *
    * @param record
    * @param now
    * @return 是否由于空间不够而不得不删除本该cache的消息
    */
  def +=(record: ConsumerRecord[Array[Byte], Array[Byte]], now: Long): Boolean
  def -=(offset: Long): this.type
}

object RecordCache {
  case class CachedAndNot(cached: List[ConsumerRecord[Array[Byte], Array[Byte]]], nonCached: Option[OffsetBatch])
  val InitialRecordCacheSize = DelayServiceConfig.MiniCacheSize * 5
//  val InitialRecordCacheSize = DelayServiceConfig.MiniCacheSize * 0
  case class CacheElement(record: ConsumerRecord[Array[Byte], Array[Byte]], size: Int, expireMs: Long)
  val CacheScaleUnit = 1024 * 1024 * 5
  val CacheScaleDownRatio = 2
  val ResizePaddingMs = 60 * Time.MsPerSec
  val ScaleUpRatio = 0.8
}
