package kafka.delay.test.unit.actor

import java.io.IOException
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.atomic.AtomicInteger

import akka.actor.{Actor, ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import kafka.delay.message.actor.NonAskTimerConsumer
import kafka.delay.message.actor.NonAskTimerConsumer.NonAskTimerConsumerConfig
import kafka.delay.message.actor.request.{BatchConsumeRequest, BatchListConsumeFailedResponse, BatchListConsumeRequest, RecordsSendResponse}
import kafka.delay.message.storage.{MetaStorage, StoreMeta}
import kafka.delay.message.timer.{PeekableMessageTimer, SystemMessageTimer}
import kafka.delay.message.timer.meta.{DelayMessageMeta, OffsetBatch, OffsetBatchList}
import kafka.delay.message.utils.SystemTime
import kafka.delay.test.unit.actor.NonAskTimerConsumerSpec._
import kafka.delay.test.unit.utils.DefaultConfig
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration._

class NonAskTimerConsumerBatchListSpec
  extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(
    """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """)))
    with FlatSpecLike
    with Matchers
    with MockFactory
    with BeforeAndAfterAll
    with ImplicitSender
    with WithExceptionProxy {

  "NonAskTimerConsumer" should "send batch list" in {
    trait MockReceive {
      def onReceive(msg: OffsetBatchList): Unit
    }

    val mockReceive = mock[MockReceive]
    (mockReceive.onReceive _)
      .expects(where { batch: OffsetBatchList => batch.batches.size == 2 })
      .once

    val messageCounter = new AtomicInteger(0)
    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchListConsumeRequest =>
          mockReceive.onReceive(request.batches)
          messageCounter.addAndGet(request.batches.batches.map(_.size).sum)
          sender() ! successfullySendResponse(request.requestId)
      }
    }

    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer", testActor)
    implicit val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    implicit val mockStorage = mock[MetaStorage]
    (mockStorage.delete(_: Seq[StoreMeta])).expects(*).atLeastOnce()
    val offsets = Seq(0l, 1L, 3L, 4L)
    val sendTime = System.currentTimeMillis()
    offsets.foreach { offset =>
      mockMetaQueue.put(DelayMessageMeta.fromClockTime(offset, sendTime + offset, SystemTime)) //make sure each meta has different expire time
    }
    implicit val timer = new PeekableMessageTimer(mockMetaQueue)
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(
            new NonAskTimerConsumerConfig(
              () => {mockStorage},
              mockMetaQueue,
              timer,
              mockMessageConsumer,
              "test",
              0,
              10000,
              DefaultConfig.config.copy(
                batchConfig = DefaultConfig.config.batchConfig.copy(
                  maxBatchDistance = 1,
                  maxBatchRange = 100000,
                  maxStashMs = 10000
                )
              )
            )
          )
        ),
        "non-ask-timer-consumer",
        testActor)))

    expectNoMsg(20 seconds)
    assert(messageCounter.get() == offsets.size)
    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)
  }

  "NonAskTimerConsumer" should "disable batch list sending when retry sending after batch list sending failed" in {
    trait MockReceive {
      def onBatchList(msg: OffsetBatchList): Unit

      def onBatch(msg: OffsetBatch): Unit
    }

    val mockReceive = mock[MockReceive]
    (mockReceive.onBatchList _)
      .expects(where { batch: OffsetBatchList => batch.batches.size == 2 })
      .once
    (mockReceive.onBatch _)
      .expects(where { batch: OffsetBatch => batch.size == 2 })
      .twice()

    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchListConsumeRequest =>
          mockReceive.onBatchList(request.batches)
          request.requestId shouldBe 1
          sender() ! BatchListConsumeFailedResponse(new IOException(), request.batches, request.requestId)
        case request: BatchConsumeRequest =>
          mockReceive.onBatch(request.batch)
          request.requestId should (be(2) or be(3))
          sender() ! RecordsSendResponse(Seq.empty, Seq.empty, request.requestId)
      }
    }

    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer", testActor)
    implicit val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    implicit val mockStorage = mock[MetaStorage]
    (mockStorage.delete(_: Seq[StoreMeta])).expects(*).atLeastOnce()
    val offsets = Seq(0l, 1L, 3L, 4L)
    val sendTime = System.currentTimeMillis()
    offsets.foreach { offset =>
      mockMetaQueue.put(DelayMessageMeta.fromClockTime(offset, sendTime + offset, SystemTime)) //make sure each meta has different expire time
    }
    implicit val timer = new PeekableMessageTimer(mockMetaQueue)
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(
            new NonAskTimerConsumerConfig(
              () =>  {mockStorage},
              mockMetaQueue,
              timer,
              mockMessageConsumer,
              "test",
              0,
              10000,
              DefaultConfig.config.copy(
                batchConfig = DefaultConfig.config.batchConfig.copy(
                  maxBatchDistance = 1,
                  maxBatchRange = 100000,
                  maxStashMs = 10000
                )
              )
            )
          )
        ),
        "non-ask-timer-consumer",
        testActor)))

    expectNoMsg(20 seconds)
    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)
  }

}