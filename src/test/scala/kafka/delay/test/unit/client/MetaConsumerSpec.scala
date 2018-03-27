package kafka.delay.test.unit.client

import java.util
import java.util.Properties

import kafka.delay.message.client.parser.KeyBasedExpireTimeParser
import kafka.delay.message.client.{MetaConsumer, OffsetReset}
import kafka.delay.message.utils.{GroupNames, TopicMapper}
import kafka.delay.test.unit.kafka.broker.SeededBroker
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalatest.{FlatSpec, Matchers}

class MetaConsumerSpec extends FlatSpec with Matchers {
  "MetaConsumer" should "poll all" in {
  val baseTopic = "test"
  val topicName = TopicMapper.getDelayTopicName(baseTopic)
  val partitionNum = 3
    val parser = new KeyBasedExpireTimeParser
    val broker = new SeededBroker(topicName, partitionNum)
    try {
      val properties = new Properties()
      properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "meta-consumer-spec")
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.metaConsumerGroup(baseTopic))
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokerConnectionString)
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetReset.earliest.value)
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](properties)


      val metaConsumer = new MetaConsumer(baseTopic, consumer)

      val delays = List(100, 300, 1100, 1200L, 1300, 2000, 3000)
      Thread.sleep(5000)
      new MockLongKeyProducer(topicName, broker.getBrokerConnectionString).sendThenClose(delays)
      println("all send")
      Thread.sleep(3100)
      var consumed = 0
      val maxAllowedPollCount = 10
      var pollCount = 0
      while (consumed != delays.size && pollCount < maxAllowedPollCount) {
        pollCount += 1
        val recordsNum = metaConsumer.poll(1000).count()
        consumed += recordsNum
      }
      metaConsumer.close()
      consumed shouldBe delays.size
    } finally {
      broker.shutdown()
    }
  }

  "MetaConsumer" should "seek" in {
    val baseTopic = "test"
    val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
    val partitionNum = 3
    val parser = new KeyBasedExpireTimeParser
    val broker = new SeededBroker(delayTopic, partitionNum)
    try {
      val properties = new Properties()
      properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "meta-consumer-spec")
      properties.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.metaConsumerGroup(baseTopic))
      properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, broker.getBrokerConnectionString)
      properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
      properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
      properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetReset.earliest.value)
      val consumer = new KafkaConsumer[Array[Byte], Array[Byte]](properties)


      val metaConsumer = new MetaConsumer(baseTopic, consumer)

      val delays = List(100, 300, 1100, 1200L, 1300, 2000)
      Thread.sleep(5000)
      new MockLongKeyProducer(delayTopic, broker.getBrokerConnectionString).sendThenClose(delays)
      println("all send")
      Thread.sleep(3100)
      var consumed = 0
      val seeks = new util.HashMap[TopicPartition, OffsetAndMetadata]
      seeks.put(new TopicPartition(delayTopic, 0), new OffsetAndMetadata(1))
      seeks.put(new TopicPartition(delayTopic, 1), new OffsetAndMetadata(1))
      seeks.put(new TopicPartition(delayTopic, 2), new OffsetAndMetadata(1))
      metaConsumer.seek(seeks)
      val maxAllowedPollCount = 10
      var pollCount = 0
      while (consumed != delays.size && pollCount < maxAllowedPollCount) {
        pollCount += 1
        val recordsNum = metaConsumer.poll(1000).count()
        consumed += recordsNum
      }
      metaConsumer.close()
      consumed shouldBe delays.size - 3
    } finally {
      broker.shutdown()
    }
  }
}
