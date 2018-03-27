package kafka.delay.message.client

import java.util.concurrent.Future

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata

trait MessageProducer {
  /**
    *
    * @param topic when to send
    * @return
    */
  def send(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]], topic: String): Seq[Future[RecordMetadata]]
  def close(timeoutMs: Long): Unit
}
