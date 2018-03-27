package kafka.delay.test.unit.actor

import java.util
import java.util.Properties

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.google.common.primitives.Longs
import com.typesafe.config.ConfigFactory
import kafka.delay.message.actor.TopicDelayService
import kafka.delay.message.utils.TopicMapper
import kafka.delay.test.unit.kafka.broker.SeededBroker
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import kafka.delay.test.unit.utils.DefaultConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._

class TopicDelayServiceSpec
  extends TestKit(
    ActorSystem("test-system",
                ConfigFactory.parseString(
                  """
                      akka.loggers = ["akka.testkit.TestEventListener"]
                  """)
                  .withFallback(ConfigFactory.load())))
    with FlatSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    //  with WithMultipleTopicsBroker
    with ImplicitSender {

  import kafka.delay.test.unit.utils.TestUtils._

  "TopicDelayService" should "be able to start" in {
    val broker = new SeededBroker(topics, true)
    //wait for delay service to start
    Thread.sleep(5000)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      val initOffsets = Map(new TopicPartition(topicName, 0) -> 0L,
                            new TopicPartition(topicName, 1) -> 0L,
                            new TopicPartition(topicName, 2) -> 0L)
      val topicDelayServiceProps = Props(
        new TopicDelayService(
          topicName,
          3,
          Some(initOffsets),
          cacheController = None,
          DefaultConfig.config.copy(bootstrapServers = bootstrapServers)))
      val topicDelayServiceProxy = system.actorOf(
        Props(new ExceptionProxy(topicDelayServiceProps, "topic-delay-service", testActor)),
        "exception-proxy")
      expectNoMsg(20 seconds)

      val probe = TestProbe()
      probe watch topicDelayServiceProxy
      system.stop(topicDelayServiceProxy)
      probe.expectTerminated(topicDelayServiceProxy, 25 seconds)
    }finally {
      broker.shutdown()
    }
  }

  "TopicDelayService" should "be able to delay single message" in {
    val broker = new SeededBroker(topics, true)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      val delayTopic = TopicMapper.getDelayTopicName(topicName)
      val expiredTopic = TopicMapper.getExpiredTopicName(topicName)
      val initOffsets = Map(new TopicPartition(delayTopic, 0) -> 0L,
                            new TopicPartition(delayTopic, 1) -> 0L,
                            new TopicPartition(delayTopic, 2) -> 0L)
      val topicDelayServiceProps = Props(
        new TopicDelayService(
          topicName,
          3,Some(initOffsets),
          cacheController = None,
          DefaultConfig.config.copy(bootstrapServers = bootstrapServers)
        ))
      val topicDelayServiceProxy = system.actorOf(
        Props(new ExceptionProxy(topicDelayServiceProps, "topic-delay-service", testActor)))
      Thread.sleep(5000)
      //wait for delay service to start
      val expiredMessageConsumer = getConsumer(bootstrapServers)
      expiredMessageConsumer.subscribe(util.Arrays.asList(expiredTopic))
      expiredMessageConsumer.poll(1000)
      new MockLongKeyProducer(delayTopic, bootstrapServers).sendThenClose(Seq(5000L))
      val sendTime = System.currentTimeMillis()
      val maxAllowedPolls = 100
      var pollCount = 0
      var found = false
      try {
        while (pollCount < maxAllowedPolls && !found) {
          val records = expiredMessageConsumer.poll(1000)
          if (records.count() != 0) {
            expiredMessageConsumer.close()
            found = true
            val record = records.iterator().next()
            record.offset() shouldBe 0
            timeShouldWithin(2000, System.currentTimeMillis(), sendTime + 5000)
          }
          pollCount += 1
        }
      } finally {
        if (!found)
          expiredMessageConsumer.close()
      }
      found shouldBe true

      val probe = TestProbe()
      probe watch topicDelayServiceProxy
      topicDelayServiceProxy ! PoisonPill
      probe.expectTerminated(topicDelayServiceProxy)
    } finally {
      broker.shutdown()
    }
  }

  "TopicDelayService" should "be able to delay multiple messages" in {
    val broker = new SeededBroker(topics, true)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      val delayTopic = TopicMapper.getDelayTopicName(topicName)
      val expiredTopic = TopicMapper.getExpiredTopicName(topicName)
      val initOffsets = Map(new TopicPartition(delayTopic, 0) -> 0L,
                            new TopicPartition(delayTopic, 1) -> 0L,
                            new TopicPartition(delayTopic, 2) -> 0L)
      val topicDelayServiceProps = Props(
        new TopicDelayService(
          topicName,
          3,
          Some(initOffsets),
          cacheController = None,
          DefaultConfig.config.copy(bootstrapServers = bootstrapServers)
        )
      )
      val topicDelayServiceProxy = system.actorOf(
        Props(
          new ExceptionProxy(
            topicDelayServiceProps,
            "topic-delay-service",
            testActor
          )
        )
      )
      //    val delays = Seq(9000, 10000L, 11000, 13000, 15000, 16000, 17000, 18000, 20000, 21000, 22000)
      val delays = Seq(10000L, 11000, 6000, 6000, 7000, 9000, 13000, 15000, 16000, 17000, 18000, 20000, 21000, 22000)
      val expiredMessageConsumer = getConsumer(bootstrapServers)
      expiredMessageConsumer.subscribe(util.Arrays.asList(expiredTopic))
      expiredMessageConsumer.poll(100)
      new MockLongKeyProducer(delayTopic, bootstrapServers).sendThenClose(delays)
      val sendTime = System.currentTimeMillis()
      var expectedMessages: mutable.ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]] = mutable.ArrayBuffer.empty
      try {
        while (System.currentTimeMillis() - sendTime < 25000) {
          val records = expiredMessageConsumer.poll(1000)
          if (records.count() != 0) {
            val iter = records.iterator()
            while (iter.hasNext) {
              val record = iter.next()
              val expireTime = Longs.fromByteArray(record.key())
              val receiveTime = record.timestamp()
              val diff = Math.abs(receiveTime - expireTime)
              val partition = record.partition()
              val offset = record.offset
              expectedMessages += record
              timeShouldWithin(3000, record.timestamp(), expireTime)
              println(s"partition: $partition, offset: $offset, expireTime: ${timeToString(expireTime) } " +
                        s"receiveTime: ${timeToString(receiveTime) }, diff: $diff ms")
            }
          }
        }
        assert(expectedMessages.size == delays.size)
      } finally {
        expiredMessageConsumer.close()
      }
      expectedMessages.size shouldBe delays.size

      val probe = TestProbe()
      probe watch topicDelayServiceProxy
      topicDelayServiceProxy ! PoisonPill
      probe.expectTerminated(topicDelayServiceProxy)
    }finally {
      broker.shutdown()
    }
  }


  private def getConsumer(bootstrapServers: String): KafkaConsumer[Array[Byte], Array[Byte]] = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "delay-service-group")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    new KafkaConsumer[Array[Byte], Array[Byte]](config)
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 20 seconds)
    super.afterAll()
  }

  private val topicName = "BaseTopic"
  private val partitionNum = 3
  private val delayTopicName = TopicMapper.getDelayTopicName(topicName)
  private val expiredTopicName = TopicMapper.getExpiredTopicName(topicName)
  private val metaTopicName = TopicMapper.getMetaTopicName(topicName)


  def topics = Map(
    delayTopicName -> partitionNum,
    expiredTopicName -> partitionNum,
    metaTopicName -> partitionNum
  )
}