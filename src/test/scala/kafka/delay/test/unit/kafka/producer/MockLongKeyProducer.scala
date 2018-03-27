package kafka.delay.test.unit.kafka.producer

import java.sql.Timestamp
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.{LongSerializer, StringSerializer}

import scala.util.Try

// producer message with expireMs as the key
class MockLongKeyProducer(targetTopic: String, bootstrapServers: String) {
  val producer = createProducer()

  def send(delays: Seq[Long], baseTime: Long = System.currentTimeMillis()): Unit = {
    delays.foreach { delay =>
      val expireMs = baseTime + delay
      val result = producer.send(new ProducerRecord[Long, String](targetTopic, expireMs, new Timestamp(expireMs).toString))
      result.get(10, TimeUnit.SECONDS)
    }
  }

  def send(expireMs: Long, value: String): Unit = {
    val record = new ProducerRecord[Long, String](targetTopic, expireMs, value)
    producer.send(record).get(10, TimeUnit.SECONDS)
  }

  def send(messages: List[(Long, String)]): Unit = {
    val futures = scala.collection.mutable.ListBuffer.empty[java.util.concurrent.Future[RecordMetadata]]
    messages.foreach {
      case (expireMs, value) => futures += producer.send(new ProducerRecord[Long, String](targetTopic, expireMs, value))
    }
    producer.flush()

    futures.foreach { future =>
      future.get()
    }
  }

  def close(): Unit = {
    producer.close(30L, TimeUnit.SECONDS)
  }

  def sendThenClose(delays: Seq[Long]) = {
    send(delays)
    close()
  }


  private def createProducer(): KafkaProducer[Long, String] = {
    val config = new Properties
    config.put(ProducerConfig.ACKS_CONFIG, "all")
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "delay-service-test")
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    config.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4")
    config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, (new RoundRobinPartitioner).getClass)
    config.put(ProducerConfig.RETRIES_CONFIG, "0")
    new KafkaProducer[Long, String](config)
  }

}
