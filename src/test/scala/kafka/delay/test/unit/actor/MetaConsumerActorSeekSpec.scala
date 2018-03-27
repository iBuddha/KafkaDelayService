package kafka.delay.test.unit.actor

import java.util.Properties

import akka.actor.{ActorSystem, PoisonPill, Props, StoppingSupervisorStrategy}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import kafka.delay.message.actor.MetaConsumerActor.{Start, Started}
import kafka.delay.message.actor.{MetaConsumerActor, MetaConsumerActorConfig}
import kafka.delay.message.client.parser.KeyBasedExpireTimeParser
import kafka.delay.message.storage.MetaStorage
import kafka.delay.message.timer.MessageTimer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.TopicMapper
import kafka.delay.test.unit.kafka.broker.SeededBroker
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import org.apache.kafka.common.TopicPartition
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class MetaConsumerActorSeekSpec extends TestKit(ActorSystem("test-system", ConfigFactory.parseProperties(MetaConsumerActorSeekSpec.config)))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender {

  "MetaConsumerActor" should "seek to init offsets then continue poll" in {
    val broker = new SeededBroker(topics, true)
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      val mockProducer = new MockLongKeyProducer(delayTopicName, bootstrapServers)
      val timer = stub[MessageTimer]
      val storage = stub[MetaStorage]
      mockProducer.send((100L until 104))
      val metaConsumerActorConfig: MetaConsumerActorConfig = MetaConsumerActorConfig(
        baseTopic = topicName,
        partitionNum = 1,
        bootstrapServers = bootstrapServers,
        expireTimeParser = new KeyBasedExpireTimeParser,
        timers = Seq(timer),
        () => {storage},
        MetaConsumerActor.defaultConsumerCreator
      )
      val consumerActor = system.actorOf(Props(classOf[MetaConsumerActor], metaConsumerActorConfig))
      val startCmd = Start(initOffsets = Map(new TopicPartition(delayTopicName, 0) -> 2))
      //    EventFilter[Exception](occurrences = 0) intercept {
      consumerActor ! startCmd
      expectMsg(60 seconds, Started)
      Thread.sleep(10000)
      mockProducer.send((0L until 3L))
      mockProducer.close()
      Thread.sleep(10000) // bigger then poll interval
      (timer.add _).verify(where { (meta: DelayMessageMeta) => meta.offset == 0 }).never()
      (timer.add _).verify(where { (meta: DelayMessageMeta) => meta.offset == 1 }).never()
      (timer.add _).verify(where { (meta: DelayMessageMeta) => meta.offset == 2 }).once
      (timer.add _).verify(where { (meta: DelayMessageMeta) => meta.offset == 3 }).once
      (timer.add _).verify(where { (meta: DelayMessageMeta) => meta.offset == 4 }).once
      val probe = TestProbe()
      probe watch consumerActor
      consumerActor ! PoisonPill
      probe.expectTerminated(consumerActor)
    } finally {
      broker.shutdown()
    }
  }


  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

  private val topicName = "BaseTopic"
  private val partitionNum = 1
  private val delayTopicName = TopicMapper.getDelayTopicName(topicName)
  private val expiredTopicName = TopicMapper.getExpiredTopicName(topicName)
  private val metaTopicName = TopicMapper.getMetaTopicName(topicName)


  def topics = Map(
    delayTopicName -> partitionNum,
    expiredTopicName -> partitionNum,
    metaTopicName -> partitionNum
  )
}

object MetaConsumerActorSeekSpec {
  val config = {
    val properties = new Properties()
    properties.put("akka.actor.guardian-supervisor-strategy", classOf[StoppingSupervisorStrategy].getCanonicalName)
    //    properties.put("akka.loggers", " [\"akka.testkit.TestEventListener\"]")
    //    properties.put("akka.loggers", List("akka.testkit.TestEventListener"))
    properties
  }
}