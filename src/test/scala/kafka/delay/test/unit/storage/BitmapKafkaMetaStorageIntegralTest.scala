package kafka.delay.test.unit.storage

import kafka.delay.message.storage.BitmapKafkaMetaStorage
import kafka.delay.message.timer.MessageTimer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.{SystemTime, TopicMapper}
import kafka.delay.test.unit.kafka.broker.SeededBroker
import kafka.delay.test.unit.kafka.producer.MockMetaProducer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

      /**
        *   override def restore(baseTopic: String,
                       utilDelayTopicOffset: Map[Int, Long],
                       timers: Seq[MessageTimer],
                       ignoredExpireBeforeMs: Long): Unit = {
        */
class BitmapKafkaMetaStorageIntegralTest extends FlatSpec
  with Matchers
  with MockFactory {

  "BitmapKafkaMetaStorage" should "collect necessary information form kafka" in {
    val baseTopic = "TEST"
    val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
    val metaTopic = TopicMapper.getMetaTopicName(baseTopic)
    val broker = new SeededBroker(Map(delayTopic -> 3, metaTopic -> 3), true)
    val bootstrapServers = broker.getBrokerConnectionString
    try {
      val producer = new MockMetaProducer(bootstrapServers)
      /**
        * meta topic offset = index
        * delay topic offset = index + 1
        * expireMs = index + 2
        */
      (0L until 100).foreach { index =>
        producer.send(metaTopic, 0, index + 1, index + 2)
      }
      val untilDelayTopicOffset: Map[Int, Long] = Map(0 -> 80)
      val ignoredExpireBeforeMs = 10L
      val mockTimer = mock[MessageTimer]
      (mockTimer.add _).expects( where { meta: DelayMessageMeta =>
        meta.offset < 80
      }
      ).repeat(71)
      val timers = Seq(mockTimer, mock[MessageTimer], mock[MessageTimer])

      val storage = new BitmapKafkaMetaStorage(bootstrapServers, baseTopic, SystemTime)
      storage.restore(baseTopic, untilDelayTopicOffset, timers, ignoredExpireBeforeMs)

    } finally {
      broker.shutdown()
    }

  }
}
