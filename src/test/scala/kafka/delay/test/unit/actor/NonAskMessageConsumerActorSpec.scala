package kafka.delay.test.unit.actor

import java.io.IOException
import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean

import akka.actor.{Actor, ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kafka.delay.message.actor.NonAskMessageConsumerActor
import kafka.delay.message.actor.request.{BatchConsumeFailedResponse, BatchConsumeRequest, RecordsSendRequest}
import kafka.delay.message.client.{ClientCreator, KafkaClientCreator, MessageConsumer}
import kafka.delay.message.timer.meta.{BitmapOffsetBatch, BitmapOffsetBatchBuilder}
import kafka.delay.message.utils._
import kafka.delay.test.unit.kafka.broker.SeededBroker
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import org.apache.kafka.clients.consumer.{Consumer, OffsetOutOfRangeException}
import org.apache.kafka.clients.producer.Producer
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class NonAskMessageConsumerActorSpec extends TestKit(ActorSystem("test-system"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender {


  "NonAskMessageConsumerActor" should "tell sender failure when consumer failed" in {
    val mockKafkaConsumer = mock[Consumer[Array[Byte], Array[Byte]]]
    (mockKafkaConsumer.assign _).expects(*).once()
    (mockKafkaConsumer.seek _).expects(*, *).onCall(_ => throw new IOException()).once()
    (mockKafkaConsumer.wakeup _).expects().once()
    (mockKafkaConsumer.close _).expects().once()

    val clientCreator = new ClientCreator {
      override def getConsumer[K, V](config: Properties): Consumer[K, V] =
        mockKafkaConsumer.asInstanceOf[Consumer[K, V]]

      override def getProducer[K, V](config: Properties): Producer[K, V] = null
    }

    val messageConsumerActor = system.actorOf(
      Props(
        new NonAskMessageConsumerActor(
          "",
          0,
          "",
          null,
          None,
          new MessageConsumer("", 0, "", clientCreator, None))
      )
    )

    val requestId = 123L

    messageConsumerActor ! BatchConsumeRequest(
      BitmapOffsetBatch(100, 200, base = 100, new util.BitSet()),
      requestId)

    val response = expectMsgClass(classOf[BatchConsumeFailedResponse])
    response.reason shouldBe a[IOException]
    response.requestId shouldBe requestId

    val probe = TestProbe()
    probe watch messageConsumerActor
    messageConsumerActor ! PoisonPill
    probe.expectTerminated(messageConsumerActor)
  }


  "NonAskMessageConsumerActor" should "forward RecordsSendRequest" in {
    val mockKafkaConsumer = mock[Consumer[Array[Byte], Array[Byte]]]
    (mockKafkaConsumer.assign _).expects(*).once()
    (mockKafkaConsumer.wakeup _).expects().once()
    (mockKafkaConsumer.close _).expects().once()

    val clientCreator = new ClientCreator {
      override def getConsumer[K, V](config: Properties): Consumer[K, V] =
        mockKafkaConsumer.asInstanceOf[Consumer[K, V]]

      override def getProducer[K, V](config: Properties): Producer[K, V] = null
    }

    val received: AtomicBoolean = new AtomicBoolean(false)

    class MockExpiredSender extends Actor {
      override def receive: Receive = {
        case _: RecordsSendRequest =>
          received.set(true)
      }
    }

    val mockExpiredSender = system.actorOf(Props(new MockExpiredSender), "sender")

    val messageConsumerActor = system.actorOf(
      Props(
        new NonAskMessageConsumerActor(
          "",
          0,
          "",
          mockExpiredSender,
          None,
          new MessageConsumer("", 0, "", clientCreator, None)
        )
      )
    )

    messageConsumerActor ! RecordsSendRequest(Seq.empty, "test", 0L)

    val probe = TestProbe()
    probe watch messageConsumerActor
    probe watch mockExpiredSender
    messageConsumerActor ! PoisonPill
    probe.expectTerminated(messageConsumerActor)
    mockExpiredSender ! PoisonPill
    probe.expectTerminated(mockExpiredSender)

    assert(received.get(), "didn't receive message should be forwarded")

  }


  "NonAskMessageConsumerActor" should "reactive to OffsetOutOfRangeException" in {
    import NonAskMessageConsumerActorSpec._
    val broker = new SeededBroker(topics, true)
    try {
      val bootstrapServers: String = broker.getBrokerConnectionString
      val nonAskMessageConsumerActor = system.actorOf(
        Props(new NonAskMessageConsumerActor(
          baseTopic = topicName,
          partition = 0,
          bootstrapServers,
          expiredSender = null,
          cacheController = None,
          new MessageConsumer(topicName, 0, bootstrapServers, KafkaClientCreator, None))
        )
      )

      new MockLongKeyProducer(TopicMapper.getDelayTopicName(topicName), bootstrapServers).sendThenClose((1L to 10).toList)
      val offsets = List(20L, 21L)
      val metaBatchBuilder = new BitmapOffsetBatchBuilder(10, 20, 3)
      offsets.foreach(metaBatchBuilder.add)
      val metaBatch = metaBatchBuilder.build()

      nonAskMessageConsumerActor ! BatchConsumeRequest(metaBatch, -1)
      val msg = expectMsgType[BatchConsumeFailedResponse]
      val BatchConsumeFailedResponse(reason, _, _) = msg
      reason shouldBe a[OffsetOutOfRangeException]
    } finally {
      broker.shutdown()
    }

  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}

object NonAskMessageConsumerActorSpec {
  val topicName = "BaseTopic"
  val partitionNum = 3
  val delayTopicName = TopicMapper.getDelayTopicName(topicName)
  val expiredTopicName = TopicMapper.getExpiredTopicName(topicName)
  val metaTopicName = TopicMapper.getMetaTopicName(topicName)


  val topics = Map(
    delayTopicName -> partitionNum,
    expiredTopicName -> partitionNum,
    metaTopicName -> partitionNum
  )
}
