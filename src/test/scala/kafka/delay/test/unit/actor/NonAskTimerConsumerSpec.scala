package kafka.delay.test.unit.actor

import java.io.IOException
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger, AtomicLong}
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.testkit.{EventFilter, ImplicitSender, TestKit, TestProbe}
import com.typesafe.config.ConfigFactory
import kafka.delay.message.actor.NonAskTimerConsumer
import kafka.delay.message.actor.NonAskTimerConsumer.NonAskTimerConsumerConfig
import kafka.delay.message.actor.request._
import kafka.delay.message.storage.{MetaStorage, StoreMeta}
import kafka.delay.message.timer._
import kafka.delay.message.timer.meta.{BitmapOffsetBatch, DelayMessageMeta, OffsetBatchList, OffsetBatch}
import kafka.delay.message.utils.SystemTime
import kafka.delay.test.unit.actor.NonAskTimerConsumerSpec._
import kafka.delay.test.unit.kafka.KafkaUtils.{newConsumerRecord, newRecordMetadata}
import kafka.delay.test.unit.utils.DefaultConfig
import org.apache.kafka.clients.producer.RecordMetadata
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.collection.mutable
import scala.concurrent.duration._
import scala.util.{Failure, Random, Success}

class NonAskTimerConsumerSpec extends TestKit(ActorSystem("test-system", ConfigFactory.parseString(
  """
  akka.loggers = ["akka.testkit.TestEventListener"]
  """)))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender
  with WithExceptionProxy {


  "nonAskTimerConsumer" should "be able to continue poll and send requests when everything is ok" in {
    implicit val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    implicit val mockStorage = mock[MetaStorage]
    (mockStorage.delete(_: Seq[StoreMeta])).expects(*).atLeastOnce()
    val offsets = Seq(0 until 200, 320 until 500)
    val secondBatch = Seq(520 until 530, 640 until 1000)
    val sendTime = System.currentTimeMillis()
    offsets.foreach { range =>
      range.foreach { offset =>
        mockMetaQueue.put(DelayMessageMeta.fromClockTime(offset, sendTime + offset, SystemTime)) //make sure each meta has different expire time
      }
    }

    trait MockReceive {
      def onReceive(msg: OffsetBatchList): Unit
    }

    val mockReceive = mock[MockReceive]
    (mockReceive.onReceive _).expects(where { batchList: OffsetBatchList => !batchList.batches.isEmpty}).twice()
    val messageCounter = new AtomicInteger(0)
    var secondBatchSent = false
    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchListConsumeRequest =>
          mockReceive.onReceive(request.batches)
          messageCounter.addAndGet(request.batches.batches.map(_.size).sum)
          sender() ! successfullySendResponse(request.requestId)
          if(!secondBatchSent) {
            secondBatchSent = true
            secondBatch.foreach { range =>
              range.foreach { offset =>
                mockMetaQueue.put(
                  DelayMessageMeta.fromClockTime(offset, System.currentTimeMillis() + offset, SystemTime)) //make sure each meta has different expire time
              }
            }
          }
      }
    }

    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer", testActor)
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(
            config(mockMessageConsumer, "test", 0)
          )
        ),
        "non-ask-timer-consumer",
        testActor)))

    expectNoMsg(5 seconds)
    print(mockMetaQueue.size())
    assert(messageCounter.get() == offsets.map(_.size).sum + secondBatch.map(_.size).sum)
    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 3 seconds)
  }


  "nonAskTimerConsumer" should "retry sending failed records when partial records are failed and after retry success, continue polling" in {

    trait MockReceive {
      def onReceiveRecordsSendRequest(request: RecordsSendRequest): Unit

      def onReceiveMetaBatch(request: BatchConsumeRequest): Unit
    }

    val mockReceive = mock[MockReceive]
    (mockReceive.onReceiveMetaBatch _)
      .expects(where { request: BatchConsumeRequest =>
        request.batch.nonEmpty && (request.requestId == 1 || request.requestId > 2) // so do have a retry
      })
      .atLeastTwice()
    (mockReceive.onReceiveRecordsSendRequest _)
      .expects(*)
      .once()

    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchConsumeRequest =>
          mockReceive.onReceiveMetaBatch(request)
          if (request.requestId == 1)
            sender() ! partialFailedSendResponse(request.requestId)
          else
            sender() ! RecordsSendResponse(Seq.empty, Seq.empty, request.requestId)
        case request: RecordsSendRequest =>
          mockReceive.onReceiveRecordsSendRequest(request)
          sender() ! RecordsSendResponse(request.records,
            Seq(Success(newRecordMetadata(topic, partition, 1))),
            request.requestId)
      }
    }

    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer-1", testActor)
    val offset = new AtomicLong(0)
    implicit val mockMetaQueue = mock[LinkedBlockingQueue[DelayMessageMeta]]
    (mockMetaQueue.poll: () => DelayMessageMeta)
      .expects()
      .onCall(() => {
        Thread.sleep(10) // sleep to avoid ScalaMock saving too many details and leading to OOM
        DelayMessageMeta.fromClockTime(offset.getAndIncrement(), SystemTime.milliseconds, SystemTime)
      })
      .atLeastOnce()
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())
    implicit val mockStorage = mock[MetaStorage]
    (mockStorage.delete(_: Seq[StoreMeta])).expects(*).atLeastOnce()
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(
            config(mockMessageConsumer, "test", 0)
          )
        ),
        "non-ask-timer-consumer-1",
        testActor)))

    expectNoMsg(10 seconds)

    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)
  }


  //mock consumer return consume failure
  //expect retry
  "NonAskTimerConsumer" should "continue retry failed BatchConsumeRequest and stop polling from timer" in {
    trait MockReceive {
      def onReceiveMetaBatch(request: BatchConsumeRequest): Unit
    }

    val mockReceive = mock[MockReceive]
    (mockReceive.onReceiveMetaBatch _)
      .expects(where { request: BatchConsumeRequest =>
        !request.batch.isEmpty
      })
      .atLeastTwice()

    val retrying: AtomicBoolean = new AtomicBoolean(false)

    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchConsumeRequest =>
          mockReceive.onReceiveMetaBatch(request)
          Thread.sleep(1000) //avoid too tight loop
          retrying.set(true)
          sender() ! BatchConsumeFailedResponse(new IOException, request.batch, request.requestId)
      }
    }

    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer-2", testActor)
    val offset = new AtomicLong(0)
    implicit val mockMetaQueue = mock[LinkedBlockingQueue[DelayMessageMeta]]
    (mockMetaQueue.poll: () => DelayMessageMeta)
      .expects()
      .onCall(() => {
        assert(retrying.get() == false, "should not polling expired message when retrying failed ones")
        Thread.sleep(1) // sleep to avoid ScalaMock saving too many details and leading to OOM
        DelayMessageMeta.fromClockTime(offset.getAndIncrement(), SystemTime.milliseconds, SystemTime)
      })
      .atLeastOnce()
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())

    //    val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    //    (0 until 1000).foreach{offset => mockMetaQueue.put(DelayMessageMeta(offset, 0L))}
    implicit val mockStorage = mock[MetaStorage]
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(
            config(
              messageConsumer = mockMessageConsumer,
              baseTopic = "test",
              partitionId = 0
            )
          )
        ),
        "non-ask-timer-consumer-1",
        testActor)))

    expectNoMsg(10 seconds)

    val probe = TestProbe()
    probe watch timerConsumer
    probe watch mockMessageConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)

    probe.expectTerminated(timerConsumer, 3 seconds)
    probe.expectTerminated(mockMessageConsumer, 3 seconds)

  }

  "NonAskTimerConsumer" should "continue retry failed RecordsSendRequest and stop polling from timer" in {
    // mock producer return send failure
    //expect retry
    trait MockMessageConsumerReceive {
      def onReceiveRecordsSendRequest(request: RecordsSendRequest): Unit

      def onReceiveMetaBatch(request: BatchConsumeRequest): Unit
    }

    /**
      * BatchConsumeRequest -> 部分失败的RecordSendResponse
      * TimerConsumer重试发送RecordSendRequest
      * 全部失败的RecordSendResponse
      * 重试...
      */

    val mockReceive = mock[MockMessageConsumerReceive]
    (mockReceive.onReceiveMetaBatch _)
      .expects(where { request: BatchConsumeRequest =>
        request.batch.nonEmpty && request.requestId == 1
      })
      .once()
    (mockReceive.onReceiveRecordsSendRequest _)
      .expects(where { request: RecordsSendRequest =>
        request.requestId >= 2
      })
      .atLeastTwice()

    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchConsumeRequest =>
          mockReceive.onReceiveMetaBatch(request)
          Thread.sleep(100)
          sender() ! partialFailedSendResponse(request.requestId)
        case request: RecordsSendRequest =>
          mockReceive.onReceiveRecordsSendRequest(request)
          assert(request.requestId >= 2)
          Thread.sleep(100)
          sender() ! completelyFailedSendResponse(request.requestId)

      }
    }
    val retrying: AtomicBoolean = new AtomicBoolean(false)
    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer-3", testActor)
    val offset = new AtomicLong(0)
    implicit val mockMetaQueue = mock[LinkedBlockingQueue[DelayMessageMeta]]
    (mockMetaQueue.poll: () => DelayMessageMeta)
      .expects()
      .onCall(() => {
        assert(retrying.get() == false, "should not polling expired message when retrying failed ones")
        Thread.sleep(1) // sleep to avoid ScalaMock saving too many details and leading to OOM
        DelayMessageMeta.fromClockTime(offset.getAndIncrement(), SystemTime.milliseconds, SystemTime)
      })
      .atLeastOnce()
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())

    //    val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    //    (0 until 1000).foreach{offset => mockMetaQueue.put(DelayMessageMeta(offset, 0L))}
    implicit val mockStorage = mock[MetaStorage]
    (mockStorage.delete(_: Seq[StoreMeta])).expects(*).atLeastOnce()
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(config(
            messageConsumer = mockMessageConsumer,
            baseTopic = "test",
            partitionId = 0
          )
          )
        ),
        "non-ask-timer-consumer-1",
        testActor)))

    expectNoMsg(10 seconds)

    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)
  }

  "NonAskTimerConsumer" should "find time out for BatchConsumeRequest, and retry it" in {
    //consumer do not return anything
    //retry
    //expect more batches and no poll from queue
    trait MockMessageConsumerReceive {
      def onReceiveRecordsSendRequest(request: RecordsSendRequest): Unit

      def onReceiveMetaBatch(request: BatchConsumeRequest): Unit
    }

    val mockReceive = mock[MockMessageConsumerReceive]
    @volatile var retryingBatch: Option[OffsetBatch] = None
    (mockReceive.onReceiveMetaBatch _)
      .expects(where { request: BatchConsumeRequest =>
        if (retryingBatch.isEmpty)
          retryingBatch = Some(request.batch)
        request.batch.nonEmpty && request.requestId >= 1 && retryingBatch.get == request.batch
      })
      .atLeastTwice()

    val retrying: AtomicBoolean = new AtomicBoolean(false)

    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchConsumeRequest =>
          mockReceive.onReceiveMetaBatch(request)
          retrying.set(true)
        //do not send back
      }
    }
    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer-4", testActor)
    val offset = new AtomicLong(0)
    implicit val mockMetaQueue = mock[LinkedBlockingQueue[DelayMessageMeta]]
    (mockMetaQueue.poll: () => DelayMessageMeta)
      .expects()
      .onCall(() => {
        assert(retrying.get() == false, "should not polling expired message when retrying failed ones")
        Thread.sleep(1) // sleep to avoid ScalaMock saving too many details and leading to OOM
        DelayMessageMeta.fromClockTime(offset.getAndIncrement(), SystemTime.milliseconds, SystemTime)
      })
      .repeat(100 until Int.MaxValue)
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())

    implicit val mockStorage = mock[MetaStorage]
    implicit val timerConsumer = system.actorOf(
      Props(
        new ExceptionProxy(
          Props(
            new NonAskTimerConsumer(
              config(
                messageConsumer = mockMessageConsumer,
                baseTopic = "test",
                partitionId = 0
              ).copy(expiredSendTimeoutMs = 2000))),
          "non-ask-timer-consumer-1",
          testActor)))

    expectNoMsg(25 seconds)

    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)

  }

  "NonAskTimerConsumer" should "find time out for RecordsSendRequest, and retry it" in {
    trait MockMessageConsumerReceive {
      def onReceiveRecordsSendRequest(request: RecordsSendRequest): Unit

      def onReceiveMetaBatch(request: BatchConsumeRequest): Unit
    }

    val mockReceive = mock[MockMessageConsumerReceive]
    (mockReceive.onReceiveMetaBatch _)
      .expects(where { request: BatchConsumeRequest =>
        request.batch.nonEmpty && request.requestId == 1
      })
      .once()

    (mockReceive onReceiveRecordsSendRequest _)
      .expects(where { request: RecordsSendRequest =>
        request.records.head.offset() == 1 && request.records.head.partition == 0
      })
      .atLeastTwice()

    val retrying: AtomicBoolean = new AtomicBoolean(false)

    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchConsumeRequest =>
          mockReceive.onReceiveMetaBatch(request)
          sender() ! partialFailedSendResponse(request.requestId)
        case request: RecordsSendRequest =>
          mockReceive.onReceiveRecordsSendRequest(request)
          retrying.set(true)
        //do nothing
      }
    }
    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer-5", testActor)
    val offset = new AtomicLong(0)
    implicit val mockMetaQueue = mock[LinkedBlockingQueue[DelayMessageMeta]]
    (mockMetaQueue.poll: () => DelayMessageMeta)
      .expects()
      .onCall(() => {
        assert(retrying.get() == false, "should not polling expired message when retrying failed ones")
        Thread.sleep(1) // sleep to avoid ScalaMock saving too many details and leading to OOM
        DelayMessageMeta.fromClockTime(offset.getAndIncrement(), SystemTime.milliseconds, SystemTime)
      })
      .repeat(100 until Int.MaxValue)
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())

    //    val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    //    (0 until 1000).foreach{offset => mockMetaQueue.put(DelayMessageMeta(offset, 0L))}
    implicit val mockStorage = mock[MetaStorage]
    (mockStorage.delete(_: Seq[StoreMeta])).expects(*).atLeastOnce()
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(new NonAskTimerConsumer(config(
          messageConsumer = mockMessageConsumer,
          baseTopic = "test",
          partitionId = 0
        ))),
        "non-ask-timer-consumer-1",
        testActor)))

    expectNoMsg(25 seconds)

    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)

  }


  "NonAskTimerConsumer" should "find time out for RecordsSendRequest, and retry it," +
    " after retry success, continue polling" in {
    trait MockMessageConsumerReceive {
      def onReceiveRecordsSendRequest(request: RecordsSendRequest): Unit

      def onReceiveMetaBatch(request: BatchConsumeRequest): Unit
    }

    val mockReceive = mock[MockMessageConsumerReceive]
    (mockReceive.onReceiveMetaBatch _)
      .expects(where { request: BatchConsumeRequest =>
        request.batch.nonEmpty && request.requestId >= 4
      })
      .atLeastTwice()

    (mockReceive.onReceiveMetaBatch _)
      .expects(where { request: BatchConsumeRequest =>
        request.batch.nonEmpty && request.requestId == 1
      })
      .once()

    (mockReceive onReceiveRecordsSendRequest _)
      .expects(where { request: RecordsSendRequest =>
        request.records.head.offset() == 1 && request.records.head.partition == 0
      })
      .atLeastTwice()


    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchConsumeRequest =>
          mockReceive.onReceiveMetaBatch(request)
          Thread.sleep(100)
          sender() ! partialFailedSendResponse(request.requestId)
        case request: RecordsSendRequest =>
          mockReceive.onReceiveRecordsSendRequest(request)
          if (request.requestId >= 3) {
            Thread.sleep(100)
            sender() ! successfullySendResponse(request.requestId)
          }
        //do nothing
      }
    }
    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer-6", testActor)
    val offset = new AtomicLong(0)
    implicit val mockMetaQueue = mock[LinkedBlockingQueue[DelayMessageMeta]]
    (mockMetaQueue.poll: () => DelayMessageMeta)
      .expects()
      .onCall(() => {
        Thread.sleep(1) // sleep to avoid ScalaMock saving too many details and leading to OOM
        DelayMessageMeta.fromClockTime(offset.getAndIncrement(), SystemTime.milliseconds, SystemTime)
      })
      .repeat(100 until Int.MaxValue)
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())

    //    val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    //    (0 until 1000).foreach{offset => mockMetaQueue.put(DelayMessageMeta(offset, 0L))}
    implicit val mockStorage = mock[MetaStorage]
    (mockStorage.delete(_: Seq[StoreMeta])).expects(*).atLeastOnce()
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(
            config(
              messageConsumer = mockMessageConsumer,
              baseTopic = "test",
              partitionId = 0
            ))),
        "non-ask-timer-consumer-1",
        testActor)))

    expectNoMsg(30 seconds)

    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)

  }


  "NonAskTimerConsumer" should "ignore expired response" in {
    trait MockMessageConsumerReceive {
      def onReceiveRecordsSendRequest(request: RecordsSendRequest): Unit

      def onReceiveMetaBatch(request: BatchConsumeRequest): Unit
    }

    val mockReceive = mock[MockMessageConsumerReceive]
    (mockReceive.onReceiveMetaBatch _).expects(*).atLeastTwice()


    class MockMessageConsumer extends Actor {
      override def receive: Receive = {
        case request: BatchConsumeRequest =>
          mockReceive.onReceiveMetaBatch(request)
          if (request.requestId == 2) {
            sender ! successfullySendResponse(1L) //send expired response
            //            sender ! BatchConsumeFailedResponse(new IOException(), request.batch, 1)
          }
      }
    }
    implicit val mockMessageConsumer = newProxy(Props(new MockMessageConsumer), "mock-message-consumer-7", testActor)
    implicit val timer = new PeekableMessageTimer(new LinkedBlockingQueue[DelayMessageMeta]())

    implicit val mockStorage = mock[MetaStorage]
    val filter = EventFilter.warning(start = "Received timeout response", occurrences = 1)
    implicit val mockMetaQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    (0 until 1000).foreach { offset =>
      mockMetaQueue.put(DelayMessageMeta.fromClockTime(offset, SystemTime.milliseconds, SystemTime))
    }
    val timerConsumer = system.actorOf(Props(
      new ExceptionProxy(
        Props(
          new NonAskTimerConsumer(
            config(
              messageConsumer = mockMessageConsumer,
              baseTopic = "test",
              partitionId = 0
            ))),
        "non-ask-timer-consumer-1",
        testActor)))

    filter.intercept {
      expectNoMsg(15 seconds)
    }
    val probe = TestProbe()
    probe watch timerConsumer
    // PoisonPill may not have good chance to come into timerConsumer's mailbox, cause its really a tight loop for
    //polling to meta-queue will always return immediately
    system.stop(timerConsumer)
    system.stop(mockMessageConsumer)
    probe.expectTerminated(timerConsumer, 10 seconds)
  }

  "NonAskTimerConsumer" should "sort and remove DelayMessageMetas" in {
    def sortedAndNonDumplicted(metas: List[DelayMessageMeta]): Boolean = {
      (metas zip metas.tail).forall(e => e._1.offset < e._2.offset)
    }
    import NonAskTimerConsumer.sortAndRemoveDuplicated
    var metas = mutable.ListBuffer.empty[DelayMessageMeta]
    sortAndRemoveDuplicated(metas.toList) shouldEqual List.empty[DelayMessageMeta]

    metas = mutable.ListBuffer.empty[DelayMessageMeta]
    (0 until 100).foreach { _ =>
      metas += DelayMessageMeta.fromClockTime(Random.nextLong(), SystemTime.milliseconds, SystemTime)
    }

    var result = sortAndRemoveDuplicated(metas.toList)
    assert(sortedAndNonDumplicted(result))

    result = List(
      DelayMessageMeta.fromClockTime(0, 0, SystemTime),
      DelayMessageMeta.fromClockTime(0, 1, SystemTime),
      DelayMessageMeta.fromClockTime(1, 1, SystemTime)
    )
    assert(sortedAndNonDumplicted(result) == false)

    metas = Random.shuffle(metas ++= metas)
    assert(sortedAndNonDumplicted(sortAndRemoveDuplicated(metas.toList)) == true)
  }


  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}

object NonAskTimerConsumerSpec {
  val topic = "test"
  val partition = 0

  val recordA = newConsumerRecord(topic, partition, 0L, "", "")
  val recordB = newConsumerRecord(topic, partition, 1L, "", "")
  val records = Seq(recordA, recordB)

  def partialFailedSendResponse(requestId: Long) = {

    val sendResult = Seq(Success(newRecordMetadata(topic, partition, 0)),
      Failure[RecordMetadata](new IOException()))
    RecordsSendResponse(records, sendResult, requestId)
  }

  def completelyFailedSendResponse(requestId: Long) = {
    val sendResult = Seq(Failure[RecordMetadata](new IOException()),
      Failure[RecordMetadata](new IOException()))
    RecordsSendResponse(records, sendResult, requestId)
  }

  def successfullySendResponse(requestId: Long) = {
    RecordsSendResponse(records,
      Seq(Success(newRecordMetadata(topic, partition, 0)), Success(newRecordMetadata(topic, partition, 1))),
      requestId)
  }

  def config(messageConsumer: ActorRef,
             baseTopic: String,
             partitionId: Int)
            (implicit metaStorage: MetaStorage,
             expiredMessageQueue: BlockingQueue[DelayMessageMeta],
             timer: PeekableMessageTimer
            ): NonAskTimerConsumerConfig = {
    NonAskTimerConsumerConfig(
      () => {metaStorage},
      expiredMessageQueue,
      timer,
      messageConsumer,
      baseTopic,
      partitionId,
      5000,
      //这样的配置，如果是每次拉mock个值放在queue里，每次会生成的是一个MetaBatch，而不是MetaBatchList
      DefaultConfig.config.copy(
        batchConfig = DefaultConfig.config.batchConfig.copy(
          dynamicBatchEnable = false,
          maxBatchDistance = 1,
          maxBatchRange = 10000,
          maxStashMs = 500,
          maxBatchSize = Int.MaxValue
        )
      )
    )
  }
}
