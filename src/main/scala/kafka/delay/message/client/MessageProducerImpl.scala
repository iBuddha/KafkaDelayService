package kafka.delay.message.client

import java.util.Properties
import java.util.concurrent.{Future, TimeUnit}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.ByteArraySerializer


class MessageProducerImpl(bootstrapServers: String) extends MessageProducer {
  private val producer = getProducer(bootstrapServers)

  /**
    * maybe block a long time
    * @return sendResult, these futures are already completed when been returned
    */
  def send(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]], topic: String): Seq[Future[RecordMetadata]] = {
    val sendResultFutures = scala.collection.mutable.ArrayBuffer.empty[Future[RecordMetadata]]
    val now = System.currentTimeMillis()
    records.foreach { record =>
      val producerRecord = new ProducerRecord(topic,
        record.partition(),
        now, //modify timestamp cause other wise may cause incorrect retention
        record.key(),
        record.value())
      val future = producer.send(producerRecord)
      sendResultFutures += future
    }
    producer.flush()
    sendResultFutures
  }

  //for test
  private def getProducer(bootstrapServers: String): Producer[Array[Byte], Array[Byte]] = {
    val config = new Properties
    config.put(ProducerConfig.ACKS_CONFIG, "all")
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[ByteArraySerializer])
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    new KafkaProducer[Array[Byte], Array[Byte]](config)
  }

   def close(timeoutMs: Long): Unit = if (producer != null) producer.close(timeoutMs, TimeUnit.MILLISECONDS)
}