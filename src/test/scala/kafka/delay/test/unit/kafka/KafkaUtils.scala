package kafka.delay.test.unit.kafka

import com.google.common.primitives._
import kafka.delay.message.timer.meta.{OffsetBatchList, OffsetBatch}
import org.apache.commons.io.Charsets
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.record.TimestampType

import scala.util.{Success, Try}

object KafkaUtils {

  def newRecordMetadata(topic: String, partition: Int, offset: Long): RecordMetadata = {
    new RecordMetadata(new TopicPartition(topic, partition), 0, offset, System.currentTimeMillis(), 0L, 2, 2)
  }

  def newConsumerRecord[K, V](topic: String, partition: Int, offset: Long, key: K, value: V): ConsumerRecord[Array[Byte], Array[Byte]] = {

    def toBytes[T](value: T): Array[Byte] = value match {
      case _: String => value.asInstanceOf[String].getBytes(Charsets.UTF_8)
      case _: Int => Ints.toByteArray(value.asInstanceOf[Int])
      case _: Long => Longs.toByteArray(value.asInstanceOf[Long])
      case _: Array[Byte] => value.asInstanceOf[Array[Byte]]
    }

    val serializedKey = toBytes(key)
    val serializedValue = toBytes(value)
    new ConsumerRecord[Array[Byte], Array[Byte]](topic, partition, offset, serializedKey, serializedValue)
  }

  /**
    * case class RecordsSendResponse(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]],
    * results: Seq[Try[RecordMetadata]],
    * requestId: Long)
    */

  def records(topic: String, partition: Int, batch: OffsetBatch): Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    batch.map { offset =>
      newConsumerRecord(topic, partition, offset, "", "")
    }
  }

  def records(topic: String, partition: Int, batchList: OffsetBatchList)
  : Seq[ConsumerRecord[Array[Byte], Array[Byte]]] = {
    batchList.batches.flatMap(batch => records(topic, partition, batch))
  }

  def successful(topic: String, partition: Int, batch: OffsetBatch): Seq[Try[RecordMetadata]] = {
    batch.map { offset =>
      Success(newRecordMetadata(topic, partition, offset))
    }
  }

  def successful(topic: String, partition: Int, batchList: OffsetBatchList): Seq[Try[RecordMetadata]] = {
    batchList.batches.flatMap(batch => successful(topic, partition, batch))
  }

  def newRecordWithSize(offset: Long,
                        keySize: Int,
                        valueSize: Int)
                       (implicit tp: TopicPartition): ConsumerRecord[Array[Byte], Array[Byte]] = {
    new ConsumerRecord[Array[Byte], Array[Byte]](
      tp.topic,
      tp.partition(),
      offset,
      0L,
      TimestampType.CREATE_TIME,
      0L,
      keySize,
      valueSize,
      new Array[Byte](keySize),
      new Array[Byte](valueSize)
    )
  }

  def newRecordWithSize(offset: Long,
                        key: Array[Byte],
                        keySize: Int,
                        value: Array[Byte],
                        valueSize: Int)
                       (implicit tp: TopicPartition): ConsumerRecord[Array[Byte], Array[Byte]] = {
    new ConsumerRecord[Array[Byte], Array[Byte]](
      tp.topic,
      tp.partition(),
      offset,
      System.currentTimeMillis(),
      TimestampType.CREATE_TIME,
      0L,
      keySize,
      valueSize,
      key,
      value
    )
  }

  def newLongKeyRecord(offset: Long, key: Long)(implicit tp: TopicPartition) = {
    newConsumerRecord(tp.topic(), tp.partition(), offset, key, "")
  }
}