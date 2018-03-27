package kafka.delay.message.client.cache

import kafka.delay.message.actor.request.ParsedRecord
import kafka.delay.message.client.parser.RecordExpireTimeParser
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable


/**
  * 用于在消息OffsetBatch或者OffsetBatchList的过程中，存储可能会需要到的消息
  * @
  */
class TempRecordCache(now: Long, parser: RecordExpireTimeParser, maxExpireMs: Long, maxSize: Int) {
  private[this] val cached = mutable.Map.empty[Long, ParsedRecord]
  private[this] var currentSize = 0

  def +=(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    if (!cached.contains(record.offset())) {
      val recordSize = RecordSizeSampler.bytes(record)
      if (recordSize + currentSize < maxSize) {
        val expireMs = parser.getExpireMs(record)
        if (expireMs < maxExpireMs) {
          cached.put(record.offset(), ParsedRecord(record, expireMs, recordSize))
          currentSize += recordSize
        }
      }
    }
  }

  def -=(offset: Long): Unit = {
    cached.get(offset).foreach { e =>
      cached.remove(offset)
      currentSize -= e.recordSize
    }
  }

  def get(offset: Long): Option[ConsumerRecord[Array[Byte], Array[Byte]]] = cached.get(offset).map(_.record)
}

