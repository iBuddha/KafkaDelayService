package kafka.delay.test.unit.actor

import java.util
import java.util.Properties

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.google.common.primitives.Longs
import com.typesafe.config.ConfigFactory
import kafka.delay.message.actor.DelayServiceActor
import kafka.delay.message.actor.DelayServiceActor.BaseTopics
import kafka.delay.message.utils.TopicMapper
import kafka.delay.test.unit.kafka.broker.SeededBroker
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import kafka.delay.test.unit.utils.DefaultConfig
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.scalamock.matchers.Matchers
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike}

import scala.collection.mutable
import scala.concurrent.duration._

class DelayServiceActorSpec
  extends TestKit(ActorSystem(
    "test-system",
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

  "DelayServiceActor" should "response to DelayTopics message" in {
    val baseTopics = Set(topicA, topicB)
    val broker = new SeededBroker(topics, true)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      val delayServiceProps = Props(
        new DelayServiceActor(
          DefaultConfig.config.copy(bootstrapServers = bootstrapServers)
        ))
      val delayServiceProxy = system.actorOf(Props(new ExceptionProxy(delayServiceProps, "delay-service", testActor)))
      expectNoMsg(30 seconds)
      delayServiceProxy ! BaseTopics(baseTopics)
      //wait for topic-delay-service to start up
      expectNoMsg(10 seconds)
      val delays = Seq(10000L, 11000, 6000, 6000, 7000, 9000)
      new MockLongKeyProducer(delayTopicA, bootstrapServers).sendThenClose(delays)
      new MockLongKeyProducer(delayTopicB, bootstrapServers).sendThenClose(delays)
      val expiredMessageConsumer = getConsumer(bootstrapServers)
      expiredMessageConsumer.subscribe(
        util.Arrays.asList(
          TopicMapper.getExpiredTopicName(topicA),
          TopicMapper.getExpiredTopicName(topicB)))
      expiredMessageConsumer.poll(100)
      val sendTime = System.currentTimeMillis()
      var expectedMessages: mutable.ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]] = mutable.ArrayBuffer.empty
      try {
        while (System.currentTimeMillis() - sendTime < 15000) {
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
              println(s"partition: $partition, offset: $offset, expireTime: ${timeToString(expireTime)} " +
                s"receiveTime: ${timeToString(receiveTime)}, diff: $diff ms")
            }
          }
        }
        assert(expectedMessages.size == delays.size * 2)
      } finally {
        expiredMessageConsumer.close()
      }
      val probe = TestProbe()
      probe watch delayServiceProxy
      // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
      //polling to meta-queue will always return immediately
      delayServiceProxy ! PoisonPill
      probe.expectTerminated(delayServiceProxy, 10 seconds)
    } finally {
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

  private val topicA = "Apple"
  private val topicB = "pear"
  private val delayTopicA = TopicMapper.getDelayTopicName(topicA)
  private val delayTopicB = TopicMapper.getDelayTopicName(topicB)
  private val expiredTopicA = TopicMapper.getExpiredTopicName(topicA)
  private val expiredTopicB = TopicMapper.getExpiredTopicName(topicB)

  private def topics = relatedTopics(topicA, 3) ++ relatedTopics(topicB, 2)

  private def relatedTopics(baseTopic: String, partitionNum: Int): Map[String, Int] = {
    val delayTopicName = TopicMapper.getDelayTopicName(baseTopic)
    val expiredTopicName = TopicMapper.getExpiredTopicName(baseTopic)
    val metaTopicName = TopicMapper.getMetaTopicName(baseTopic)
    Map(
      delayTopicName -> partitionNum,
      expiredTopicName -> partitionNum,
      metaTopicName -> partitionNum
    )
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system, 20 seconds)
    super.afterAll()
  }
}
