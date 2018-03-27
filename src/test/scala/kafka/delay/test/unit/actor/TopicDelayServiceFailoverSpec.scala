package kafka.delay.test.unit.actor

import java.io.IOException
import java.sql.Timestamp
import java.util
import java.util.Properties

import akka.actor.{ActorInitializationException, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.google.common.primitives.Longs
import com.typesafe.config.ConfigFactory
import kafka.delay.message.actor.TopicDelayService
import kafka.delay.message.utils.{GroupNames, TopicMapper}
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

class TopicDelayServiceFailoverSpec extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """).withFallback(ConfigFactory.load())))
                                            with FlatSpecLike
                                            with Matchers
                                            with MockFactory
                                            with BeforeAndAfterAll
                                            //  with WithMultipleTopicsBroker
                                            with ImplicitSender {

  /**
    * 1. send some messages
    * 2. wait all expired messages to be fetched
    * 3. stop topic delay service
    * 4. send more messages
    * 5. start topic delay service
    * 6. expected all messages are fetched exactly once
    */
    "TopicDelayService" should "continue from continue consume delay topic from last committed position" in {
    val broker = new SeededBroker(topics, true)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      //wait for delay service to start
      Thread.sleep(10000)
      val initOffsets = Map(new TopicPartition(delayTopicName, 0) -> 0L,
        new TopicPartition(delayTopicName, 1) -> 0L,
        new TopicPartition(delayTopicName, 2) -> 0L)
      var topicDelayServiceProps = Props(
        new TopicDelayService(
          topicName,
          3,
          Some(initOffsets),
          cacheController = None,
          DefaultConfig.config.copy(
            batchConfig = DefaultConfig.config.batchConfig.copy(dynamicBatchEnable = false),
            bootstrapServers = bootstrapServers)
        )
      )
      var topicDelayService = system.actorOf(topicDelayServiceProps, "topic-delay-service")
      val expiredMessageConsumer = getConsumer(bootstrapServers)
      expiredMessageConsumer.subscribe(util.Arrays.asList(expiredTopicName))
      expiredMessageConsumer.poll(1000)
      val firstDelays = Seq(10000L, 11000, 6000, 6000, 7000, 30000, 35000)
      new MockLongKeyProducer(delayTopicName, bootstrapServers).sendThenClose(firstDelays)
      var sendTime = System.currentTimeMillis()
      var expectedMessages: mutable.ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]] = mutable.ArrayBuffer.empty
      try {
        while (System.currentTimeMillis() - sendTime < 11000 + 3000) {
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
        assert(expectedMessages.size == 5)

        val probe = TestProbe()
        probe watch topicDelayService
        topicDelayService ! PoisonPill
        probe.expectTerminated(topicDelayService, 5 seconds)

//        val latestCommittedOffsetAndMeta = KafkaAdmin.latestCommittedOffset(
//          bootstrapServers,
//          delayTopicName,
//          3,
//          GroupNames.metaConsumerGroup(topicName))
//        val latestCommitted = latestCommittedOffsetAndMeta.map {
//          case (partitionId, offsetAndMetadataOpt) =>
//            (new TopicPartition(delayTopicName, partitionId), offsetAndMetadataOpt.get.offset)
//        }
        topicDelayServiceProps = Props(
          new TopicDelayService(
            topicName,
            3,
            None,
            cacheController = None,
            DefaultConfig.config.copy(
              batchConfig = DefaultConfig.config.batchConfig.copy(
              dynamicBatchEnable = false),
              bootstrapServers = bootstrapServers)
          )
        )
        val secondDelays = Seq(5000L, 6000, 7000)
        new MockLongKeyProducer(delayTopicName, bootstrapServers).sendThenClose(secondDelays)
        sendTime = System.currentTimeMillis()
        expectedMessages = mutable.ArrayBuffer.empty
        topicDelayService = system.actorOf(topicDelayServiceProps, "topic-delay-service")
        while (System.currentTimeMillis() - sendTime < 30000) {
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
        assert(expectedMessages.size == 5)

        probe watch topicDelayService
        topicDelayService ! PoisonPill
        probe.expectTerminated(topicDelayService, 10 seconds)

      } finally {
        expiredMessageConsumer.close()
      }
    } finally {
      broker.shutdown()
    }
  }

  /**
    * 1. start delay service
    * 2. fetch messages and add to timer
    * 3. shutdown broker
    * 4. expect no children terminated
    */
  "TopicDelayService" should "continue working after broker failure" in {
    var broker = new SeededBroker(topics, true)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      val initOffsets = Map(new TopicPartition(delayTopicName, 0) -> 0L,
                            new TopicPartition(delayTopicName, 1) -> 0L,
                            new TopicPartition(delayTopicName, 2) -> 0L)
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
          new ExceptionProxy(topicDelayServiceProps,
                             "topic-delay-service",
                             testActor)))
      Thread.sleep(15000)
      val secondDelays = Seq(15000L, 20000)
      new MockLongKeyProducer(delayTopicName, bootstrapServers).sendThenClose(secondDelays)
      expectNoMsg(5 seconds)
      val oldBroker = broker
      broker = null
      oldBroker.shutdown()
      println("broker has shutdown")
      expectNoMsg(30 seconds)
    } finally {
      if (broker != null)
        broker.shutdown()
    }
  }

  "TopicDelayService" should "fail when can't connect to kafka when starting" in {
    val bootstrapServers = "foo:9999"
    val initOffsets = Map(
      new TopicPartition(delayTopicName, 0) -> 0L,
      new TopicPartition(delayTopicName, 1) -> 0L,
      new TopicPartition(delayTopicName, 2) -> 0L
    )
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
        new ExceptionProxy(topicDelayServiceProps,
                           "topic-delay-service",
                           testActor)))
    expectMsgClass[ActorInitializationException](5 seconds, classOf[ActorInitializationException])
  }

  private def timeToString(timestamp: Long) = new Timestamp(timestamp).toString

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

  private def timeShouldWithin(maxDiff: Long, firstTime: Long, secondTime: Long) = {
    if (Math.max(maxDiff, Math.abs(firstTime - secondTime)) != maxDiff)
      println(s"firstTime: ${timeToString(firstTime) }, secondTime: ${timeToString(secondTime) }")
    Math.max(maxDiff, Math.abs(firstTime - secondTime)) shouldBe maxDiff
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
