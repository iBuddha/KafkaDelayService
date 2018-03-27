package kafka.delay.test.unit.actor

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.delay.message.actor.cached.CacheAwareMessageConsumerActor
import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.actor.request._
import kafka.delay.message.client.MetaBasedConsumer
import kafka.delay.message.client.cache.TempRecordCache
import kafka.delay.message.timer.meta.{ArrayOffsetBatch, OffsetBatch, OffsetBatchList}
import kafka.delay.test.unit.kafka.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.TopicPartition
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.util.{Success, Try}

class CacheAwareMessageConsumerActorSpec extends TestKit(ActorSystem("test-system"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender {

  "cache-aware message consumer actor" should "work with cache actor" in {
    val baseTopic = "foo"
    val partition = 0
    implicit val tp = new TopicPartition(baseTopic, partition)
    val cacheController = testActor

    val messageConsumer = new MetaBasedConsumer {

      override def resetCacheSize(size: Int, version: Long): Unit = {}

      /**
        * 阻塞的
        *
        * @param metaBatch
        * @return
        */
      override def consume(metaBatch: OffsetBatch): BatchConsumeResult = {
        throw new UnsupportedOperationException
      }

      override def consume(metaBatches: OffsetBatchList): BatchConsumeResult = {
        throw new UnsupportedOperationException
      }

      override def consume(metaBatch: OffsetBatch, cached: TempRecordCache): BatchConsumeResult = {
        val records = metaBatch.map{offset =>
          KafkaUtils.newRecordWithSize(offset, 0, 0)
        }.toList
        BatchConsumeResult(records, None)
      }

      override def needMoreCacheSpace: Boolean = false

      override def getCacheMetrics(): Option[RecordCacheMetrics] = None

      override def close(): Unit = {}

      /**
        * 加一些记录到cache
        *
        * @param records
        */
      override def addToCache(records: List[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit =
        throw new UnsupportedOperationException
    }

    class MockExpiredSender extends Actor {
      override def receive = {
        case RecordsSendRequest(consumeResult, targetTopic, requestId) =>
          val sendResults: Seq[Try[RecordMetadata]] = consumeResult.map { record =>
            Success(KafkaUtils.newRecordMetadata(record.topic(), record.partition(), record.offset()))
          }
          sender ! RecordsSendResponse(consumeResult, sendResults, requestId)
      }
    }

    val expiredSenderActor = system.actorOf(Props(new MockExpiredSender))

    class MockController extends Actor {
      override def receive = {
        case _ =>
      }
    }

    val controllerActor = system.actorOf(Props(new MockController))

    val messageConsumerActor = system.actorOf(
      Props(
        new CacheAwareMessageConsumerActor(
          baseTopic,
          partition,
          "",
          expiredSenderActor,
          controllerActor,
          messageConsumer
        )
      )
    )

    val preConsumedBatch = new ArrayOffsetBatch(Array(0L, 3L, 4, 5))
    messageConsumerActor ! BatchConsumeRequest(preConsumedBatch, 0)

//    val recordsSend = expectMsgType[RecordsSendResponse](FiniteDuration(10, TimeUnit.MINUTES))
    expectMsgType[MessageConsumeComplete]
    var recordsSend = expectMsgType[RecordsSendResponse]
    recordsSend.requestId shouldBe 0
    recordsSend.results.map(_.get.offset()).toSeq shouldEqual Seq(0L, 3, 4, 5)
    println("sending BatchListConsumeRequest")
    messageConsumerActor ! BatchListConsumeRequest(OffsetBatchList(List(preConsumedBatch)), 1)
    expectMsgType[MessageConsumeComplete]
    recordsSend = expectMsgType[RecordsSendResponse]
    recordsSend.requestId shouldBe 1
    recordsSend.results.map(_.get.offset()).toSeq shouldEqual Seq(0L, 3, 4, 5)

  }

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

}
