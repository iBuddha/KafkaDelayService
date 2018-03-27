package kafka.delay.message.client

import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.actor.request.BatchConsumeResult
import kafka.delay.message.client.cache.TempRecordCache
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import org.apache.kafka.clients.consumer.ConsumerRecord

trait MetaBasedConsumer extends AutoCloseable {
  /**
    * 阻塞的
    * @param metaBatch
    * @return
    */
  def consume(metaBatch: OffsetBatch): BatchConsumeResult

  def consume(metaBatches: OffsetBatchList): BatchConsumeResult


  /**
    *
    * @param metaBatch
    * @param cached 对于没有超时，并且没有超过maxExpireTime的消息，会放在cached这个集合里，因为它可能会在接下来被消费到
    * @return
    */
  def consume(metaBatch: OffsetBatch,
              cached: TempRecordCache): BatchConsumeResult

  def resetCacheSize(size: Int, version: Long): Unit

  def getCacheMetrics(): Option[RecordCacheMetrics]

  def needMoreCacheSpace: Boolean

  /**
    * 加一些记录到cache
    * @param records
    */
  def addToCache(records: List[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit
}
