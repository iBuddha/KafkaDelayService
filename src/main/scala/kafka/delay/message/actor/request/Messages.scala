package kafka.delay.message.actor.request

import akka.actor.ActorRef
import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.timer.meta.{BitmapOffsetBatch, OffsetBatchList, OffsetBatch}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition

import scala.util.Try

//用于NonAskTimerConsumer

case class BatchConsumeRequest(batch: OffsetBatch, requestId: Long)

case class BatchListConsumeRequest(batches: OffsetBatchList, requestId: Long)

case class BatchConsumeResult(fetched: Seq[ConsumerRecord[Array[Byte], Array[Byte]]],
                              missed: Option[Traversable[Long]])

case class PreFetchBatchRequest(batch: OffsetBatch, requestId: Long)
case class PreFetchBatchListRequest(batches: OffsetBatchList)
case class PreFetchRequest(batches: OffsetBatchList, maxExpireMs: Long)
case class MessageConsumeComplete(requestId: Long)

/**
  * Message Consumer发送，用于表示消息的平均大小。主要用于决定将要抓取的batch的大小
  * @param value
  */
case class AverageRecordSize(value: Int)

case class BatchConsumeFailedResponse(reason: Throwable, batch: OffsetBatch, requestId: Long)

/**
  * 并不代表所有批次都失败了，处理者需要重试单个批次
  */
case class BatchListConsumeFailedResponse(reason: Throwable, batches: OffsetBatchList, requestId: Long)

case class PermanentlyMissedMessages(missed: Traversable[Long])

case class RecordsSendRequest(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]],
                              targetTopic: String,
                              requestId: Long) {
  override def toString(): String = {
    s"RecordsSendRequest(size: ${records.size}, requestId: $requestId)"
  }
}

case class RecordsSendResponse(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]],
                               results: Seq[Try[RecordMetadata]],
                               requestId: Long)