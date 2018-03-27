package kafka.delay.test.unit.client

import kafka.delay.message.client.{KafkaClientCreator, MessageConsumer}
import kafka.delay.message.exception.ImpossibleBatchException
import kafka.delay.message.timer.meta.BitmapOffsetBatchBuilder
import kafka.delay.message.utils.TopicMapper
import kafka.delay.test.unit.kafka.broker.{SeededBroker, WithSingleTopicBroker}
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException
import org.scalatest.{FlatSpec, Matchers}

class MessageConsumerSpec extends FlatSpec with Matchers {
  "MessageConsumer" should "get all messages in a MetaBatch" in {
    val broker = new SeededBroker(topicName, partitionNum)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      new MockLongKeyProducer(TopicMapper.getDelayTopicName(topicName), bootstrapServers).sendThenClose((1L to 1000).toList)
      val consumer = new MessageConsumer(topicName, 0, bootstrapServers, KafkaClientCreator, None)
      try {
        val offsets = List(0L, 2, 3, 5, 7)
        val metaBatchBuilder = new BitmapOffsetBatchBuilder(10, 0, 3)
        offsets.foreach(metaBatchBuilder.add)
        val metaBatch = metaBatchBuilder.build()
        val records = consumer.consume(metaBatch)
        records.fetched.map(_.offset()).toSet shouldEqual offsets.toSet
      } finally {
        consumer.close()
      }
    } finally {
      broker.shutdown()
    }
  }

  "MessageConsumer" should "throw exception when seek to non-existed offset" in {
    val broker = new SeededBroker(topicName, partitionNum)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      new MockLongKeyProducer(TopicMapper.getDelayTopicName(topicName), bootstrapServers).sendThenClose((1L to 10).toList)
      val consumer = new MessageConsumer(topicName, 0, bootstrapServers, KafkaClientCreator, None)
      try {
        val offsets = List(100 * 1000L)
        val metaBatchBuilder = new BitmapOffsetBatchBuilder(10, 100 * 1000, 3)
        offsets.foreach(metaBatchBuilder.add)
        val metaBatch = metaBatchBuilder.build()
        intercept[OffsetOutOfRangeException] {
          consumer.consume(metaBatch)
        }
      } finally {
        consumer.close()
      }
    } finally {
      broker.shutdown()
    }
  }

  "MessageConsumer" should "throw exception when batch is in-completable" in {
    val broker = new SeededBroker(topicName, partitionNum)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      new MockLongKeyProducer(TopicMapper.getDelayTopicName(topicName), bootstrapServers).sendThenClose((0L to 10).toList)
      val consumer = new MessageConsumer(topicName, 0, bootstrapServers, KafkaClientCreator, None)
      try {
        val offsets = List(7L, 8, 9, 10, 11, 12)
        val metaBatchBuilder = new BitmapOffsetBatchBuilder(10, 7, 1)
        offsets.foreach(metaBatchBuilder.add)
        val metaBatch = metaBatchBuilder.build()
        intercept[ImpossibleBatchException] {
          consumer.consume(metaBatch)
        }
      } finally {
        consumer.close()
      }
    } finally {
      broker.shutdown()
    }
  }

  val topicName: String = "test-topic"

  val partitionNum: Int = 3
}
