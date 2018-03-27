package kafka.delay.test.unit.kafka.producer

import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.delay.message.storage.AbstractKafkaMetaStorage.MetaKey
import kafka.delay.message.storage.MetaKeySerializer
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.LongSerializer

class MockMetaProducer(bootstrapServers: String) {

  private[this] val producer = createProducer()

  def send(topic: String, partition: Int, delayTopicOffset: Long, expireMs: java.lang.Long): Unit = {
    producer.send(new ProducerRecord[MetaKey, java.lang.Long](topic, partition, MetaKey(partition, delayTopicOffset), expireMs)).get(10, TimeUnit.SECONDS)
  }

  private def createProducer(): KafkaProducer[MetaKey, java.lang.Long] = {
    val config = new Properties
    config.put(ProducerConfig.ACKS_CONFIG, "all")
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "delay-service-test")
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[MetaKeySerializer])
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer])
    config.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, (new RoundRobinPartitioner).getClass)
    config.put(ProducerConfig.RETRIES_CONFIG, "0")
    new KafkaProducer[MetaKey, java.lang.Long](config)
  }

}
