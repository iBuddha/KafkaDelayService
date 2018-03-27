package kafka.delay.test.unit.actor

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.delay.message.actor.DelayServiceActor.BaseTopics
import kafka.delay.message.actor.DelayTopicListener
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.mutable
import kafka.delay.message.utils.TopicMapper._
import kafka.delay.test.unit.kafka.broker.SeededBroker

import scala.concurrent.duration._

class DelayTopicListenerSpec extends TestKit(ActorSystem("test-system"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender {

  "DelayTopicListener" should "send all qualified topics" in {
    val baseTopicA = "apple"
    val baseTopicB = "pear"
    val baseTopicC = "banana"
    val topics = mutable.Map.empty[String, Int]
    topics += (getDelayTopicName(baseTopicA) -> 3)
    topics += (getExpiredTopicName(baseTopicA) -> 3)
    topics += (getMetaTopicName(baseTopicA) -> 3)
    topics += (getExpiredTopicName(baseTopicB) -> 3)
    topics += (getMetaTopicName(baseTopicB) -> 3)
    topics += (getDelayTopicName(baseTopicC) -> 3)
    topics += (getExpiredTopicName(baseTopicC) -> 3)
    topics += (getMetaTopicName(baseTopicC) -> 3)

    val broker = new SeededBroker(topics.toMap, true)
    //wait broker to start up
    Thread.sleep(10000)

    class MockDelayServiceActor(msgActor: ActorRef) extends Actor {
      override def receive = {
        case msg: BaseTopics =>
          msgActor.forward(msg)
      }
    }
    try {
      val bootstrapServers = broker.getBrokerConnectionString
      val delayService = system.actorOf(Props(new MockDelayServiceActor(testActor)), "delay-service")
      val delayTopicListenerProps = Props(
        classOf[DelayTopicListener],
        delayService,
        1 minutes,
        bootstrapServers)
      val proxy = system.actorOf(
        Props(new ExceptionProxy(delayTopicListenerProps, "delay-topic-listener", testActor)))

      expectMsg(2 minutes, BaseTopics(Set(baseTopicA, baseTopicC)))

    } finally {
      broker.shutdown()
    }
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}
