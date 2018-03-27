package kafka.delay.test.product

import kafka.delay.message.utils.{Time, TopicMapper}
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import kafka.delay.test.unit.utils.TestUtils

import scala.util.Random

/**
  * 用于往指定的topic发送一批delay message， 可以指定消息的数量，以及延迟的范围
  */
object LongDelayTest extends App {

  private val bootstrapServers = "slave1.test:9092"
  private val baseTopic = "foo"
  private val messageNumber = 10000 //要发送的消息数量
  private val maxDelay = Time.MsPerSec * Time.SecsPerMin * 1 //用于指定延迟的范围
  private val messageSize = 1024

  private val delayTopic = TopicMapper.getDelayTopicName(baseTopic)

  private val producer = new MockLongKeyProducer(delayTopic, bootstrapServers)

  private def nextExpireMs = Random.nextInt(maxDelay).toLong + System.currentTimeMillis()

  private var sendCount = 0

  private val paddingString = "K" * messageSize
  try {
    (0 until messageNumber).grouped(10000).foreach { group =>
      var batch = List.empty[(Long, String)]
      group.foreach { _ =>
        val expireMs = nextExpireMs
        batch = batch :+ (expireMs, TestUtils.timeToString(expireMs) + paddingString)
      }
      producer.send(batch)
      sendCount += batch.size
      println(s"current sent count $sendCount")
    }
  } catch{
    case e: Exception => e.printStackTrace()
  } finally{
    producer.close()
  }

}
