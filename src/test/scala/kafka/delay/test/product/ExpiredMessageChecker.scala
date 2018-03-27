package kafka.delay.test.product

import java.util
import java.util.Properties

import kafka.delay.message.utils.TopicMapper
import kafka.delay.test.unit.utils.TestUtils
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, ConsumerRecords, KafkaConsumer}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}


/**
  * 用于检查收到的expired message是否是在某个可允许的时间范围之内到达的expired topic。
  * 用于检查delay service的精确度
  */
object ExpiredMessageChecker extends App {

  private val bootstrapServers = "slave1.test:9092"
  private val baseTopic = "foo"
  private val expiredTopic = TopicMapper.getExpiredTopicName(baseTopic)

  var consumerOpt: Option[KafkaConsumer[Long, String]] = None
  try {
    consumerOpt = Some(new KafkaConsumer[Long, String](properties))
    val consumer = consumerOpt.get
    consumer.subscribe(util.Arrays.asList(expiredTopic))
    while (!Thread.currentThread().isInterrupted) {
      val records = consumer.poll(10000)
      println(s"received records of size: ${records.count()}")
      check(records, 100)
    }
  } finally {
    consumerOpt.foreach(_.close)
  }

  private def check(records: ConsumerRecords[Long, String], expectedDiff: Long): Unit = {
    val iterator = records.iterator()
    while (iterator.hasNext) {
      check(iterator.next, expectedDiff)
    }
  }

  private def check(record: ConsumerRecord[Long, String], expectedDiff: Long): Unit = {
    val expireMs = record.key()
    val recordReceiveMs = record.timestamp()

    val expireTime = TestUtils.timeToString(expireMs)
    val recordReceiveTime = TestUtils.timeToString(recordReceiveMs)

    val diff = Math.abs(expireMs - recordReceiveMs)
    val partition = record.partition()
    val offset = record.offset()
    val currentMs = System.currentTimeMillis()
    val currentTime = TestUtils.timeToString(currentMs)
    if (diff > expectedDiff)
      println(s"expireTime: $expireTime, receiveTime: $recordReceiveTime, diff: $diff, " +
        s"partition: $partition, offset: $offset, currentTime: $currentTime")
  }


  private def properties: Properties = {
    val config = new Properties()
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service-test")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "delay-service-test-expired-message-checker")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    config
  }
}
