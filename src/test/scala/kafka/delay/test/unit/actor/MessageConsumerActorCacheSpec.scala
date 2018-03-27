package kafka.delay.test.unit.actor

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.delay.message.client.cache.RecordCache._
import kafka.delay.message.actor.NonAskMessageConsumerActor
import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.actor.request._
import kafka.delay.message.client.MetaBasedConsumer
import kafka.delay.message.client.cache.{RecordCache, TempRecordCache}
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import kafka.delay.test.unit.kafka.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class MessageConsumerActorCacheSpec extends TestKit(ActorSystem("test-system"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender {

  "MessageConsumerActor" should "request more cache space" in {
    implicit val tp = new TopicPartition("test", 0)

    val initCacheSize = 1024

    class MockMessageConsumer extends MetaBasedConsumer {
      /**
        * 阻塞的
        *
        * @param metaBatch
        * @return
        */
      override def consume(metaBatch: OffsetBatch): BatchConsumeResult =
        BatchConsumeResult(
          Seq.fill(NonAskMessageConsumerActor.RecordSizeUpdateInterval + 1)(KafkaUtils.newRecordWithSize(1, 100, 100)(tp)),
          None)

      override def consume(metaBatches: OffsetBatchList): BatchConsumeResult =
        throw new UnsupportedOperationException

      override def resetCacheSize(size: Int, version: Long): Unit = {}

      override def getCacheMetrics(): Option[RecordCacheMetrics] =
        Some(RecordCacheMetrics(
          performancePoint = 1.0,
          currentCacheSize = initCacheSize,
          usedCacheSize = (initCacheSize * (NonAskMessageConsumerActor.scaleUpRatio + 0.1)).toInt,
          version = 1))

      override def needMoreCacheSpace: Boolean = true

      override def close(): Unit = {}

      /**
        *
        * @param metaBatch
        * @param cached 对于没有超时，并且没有超过maxExpireTime的消息，会放在cached这个集合里，因为它可能会在接下来被消费到
        * @return
        */
      override def consume(metaBatch: OffsetBatch, cached: TempRecordCache) =
        throw new UnsupportedOperationException

      /**
        * 加一些记录到cache
        *
        * @param records
        */
      override def addToCache(records: List[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit =
        throw new UnsupportedOperationException
    }


    class MockExpiredSender extends Actor {
      override def receive: Receive = {
        case _: RecordsSendRequest =>
      }
    }

    val mockExpiredSender = system.actorOf(Props(new MockExpiredSender))
    val messageConsumerActor = system.actorOf(
      Props(
        new NonAskMessageConsumerActor(
          baseTopic = tp.topic,
          partition = tp.partition,
          "",
          expiredSender = mockExpiredSender,
          Some(testActor),
          new MockMessageConsumer
        )))
    messageConsumerActor.tell(CheckCacheStateRequest, testActor)
    val updateRequest = expectMsgType[UpdateCacheStateRequest]
    updateRequest.state.currentSize shouldBe initCacheSize
    val spaceRequest = expectMsgType[CacheSpaceRequest]
    spaceRequest.targetSize shouldBe initCacheSize + CacheScaleUnit
  }

  "MessageConsumerActor" should "request less cache space" in {
    implicit val tp = new TopicPartition("test", 0)

    val metrics = RecordCacheMetrics(
      performancePoint = 1.0,
      currentCacheSize = CacheScaleUnit * (CacheScaleDownRatio + 1),
      usedCacheSize = CacheScaleUnit,
      version = 1)

    class MockMessageConsumer extends MetaBasedConsumer {
      /**
        * 阻塞的
        *
        * @param metaBatch
        * @return
        */
      override def consume(metaBatch: OffsetBatch): BatchConsumeResult =
        BatchConsumeResult(
          Seq.fill(NonAskMessageConsumerActor.RecordSizeUpdateInterval + 1)(KafkaUtils.newRecordWithSize(1, 100, 100)(tp)),
          None)

      override def consume(metaBatches: OffsetBatchList): BatchConsumeResult =
        throw new UnsupportedOperationException

      override def resetCacheSize(size: Int, version: Long): Unit = {}

      override def getCacheMetrics(): Option[RecordCacheMetrics] = Some(metrics)

      override def needMoreCacheSpace: Boolean = false

      override def close(): Unit = {}

      /**
        *
        * @param metaBatch
        * @param cached 对于没有超时，并且没有超过maxExpireTime的消息，会放在cached这个集合里，因为它可能会在接下来被消费到
        * @return
        */
      override def consume(metaBatch: OffsetBatch, cached: TempRecordCache) =
        throw new UnsupportedOperationException

      /**
        * 加一些记录到cache
        *
        * @param records
        */
      override def addToCache(records: List[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit =
        throw new UnsupportedOperationException
    }


    class MockExpiredSender extends Actor {
      override def receive: Receive = {
        case _: RecordsSendRequest =>
      }
    }

    val mockExpiredSender = system.actorOf(Props(new MockExpiredSender))
    val messageConsumerActor = system.actorOf(
      Props(
        new NonAskMessageConsumerActor(
          baseTopic = tp.topic,
          partition = tp.partition,
          "",
          expiredSender = mockExpiredSender,
          Some(testActor),
          new MockMessageConsumer
        )))

    messageConsumerActor.tell(CheckCacheStateRequest, testActor)
    val updateRequest = expectMsgType[UpdateCacheStateRequest]
    updateRequest.state.currentSize shouldBe metrics.currentCacheSize
    val spaceRequest = expectMsgType[CacheSpaceRequest]
    spaceRequest.targetSize shouldBe metrics.currentCacheSize - CacheScaleUnit
  }

  override def afterAll() = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

}
