package kafka.delay.test.unit.actor

import java.io.IOException

import akka.actor.{ActorSystem, PoisonPill, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import kafka.delay.message.actor.ExpiredSenderActor
import kafka.delay.message.actor.request.{RecordsSendRequest, RecordsSendResponse}
import kafka.delay.message.client.MessageProducer
import kafka.delay.message.utils.TopicMapper
import kafka.delay.test.unit.kafka.KafkaUtils._
import kafka.delay.test.unit.utils.JavaFutures
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._
import scala.util.Random

class ExpiredSenderActorSpec extends TestKit(ActorSystem("test-system"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender {
  "ExpiredSenderActor" should "correctly response to RecordSendRequest" in {
    val baseTopic = "test"
    val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
    val expireTopic = TopicMapper.getExpiredTopicName(baseTopic)
    val partition = 0
    val recordA = newConsumerRecord(delayTopic, partition, 0L, "", "")
    val recordB = newConsumerRecord(delayTopic, partition, 1L, "", "")

    val sendResult = Seq(JavaFutures.success(newRecordMetadata(baseTopic, partition, 0)),
      JavaFutures.failed[RecordMetadata](new IOException()))
    val producer = mock[MessageProducer]
    (producer.send _).expects(
      where { (records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]], targetTopic: String) =>
        records.size == 2 && targetTopic == expireTopic
      })
      .returns(sendResult)
    (producer.close(_: Long)).expects(*)

    val senderActor = system.actorOf(Props(new ExpiredSenderActor("", _ => producer)))
    val requestId = Random.nextLong()
    senderActor ! RecordsSendRequest(Seq(recordA, recordB), expireTopic, requestId)
    //    val expected = Seq(Success(newRecordMetaData(topic, partition, 0)), Success(newRecordMetaData(topic, partition, 1)))
    expectMsgPF(10 seconds) {
      case result: RecordsSendResponse =>
        result.requestId shouldBe requestId
        val results = result.results
        results(0).isFailure shouldBe false
        results(0).get.offset() shouldBe 0
        results(1).isFailure shouldBe true
        results(1).failed.get shouldBe a[IOException]
    }
    val probe = TestProbe()
    probe watch senderActor
    senderActor ! PoisonPill
    probe.expectTerminated(senderActor)
  }


  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}

