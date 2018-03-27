package kafka.delay.message.actor.cached

import java.util.concurrent.{BlockingQueue, TimeUnit}

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import kafka.delay.message.actor.cached.TimerConsumer.TimerConsumerConfig
import kafka.delay.message.actor.request._
import kafka.delay.message.client.cache.RecordCache
import kafka.delay.message.exception.ImpossibleBatchException
import kafka.delay.message.storage.{MetaStorage, StoreMeta}
import kafka.delay.message.timer.PeekableMessageTimer
import kafka.delay.message.timer.meta.{DelayMessageMeta, OffsetBatch, OffsetBatchBuilder, OffsetBatchList}
import kafka.delay.message.utils.{DelayServiceConfig, Size, SystemTime, TopicMapper}
import org.apache.kafka.clients.consumer.{ConsumerRecord, OffsetOutOfRangeException}

import scala.collection.{SortedSet, mutable}
import scala.concurrent.duration._

class TimerConsumer(timerConsumerConfig: TimerConsumerConfig)
  extends Actor with ActorLogging {
  private implicit val executionContext = context.dispatcher

  import TimerConsumer._

  private val expiredMessageQueue = timerConsumerConfig.expiredMessageQueue
  private val timer = timerConsumerConfig.timer
  private val messageConsumer: ActorRef = timerConsumerConfig.messageConsumer
  private val requestTimeoutMs = timerConsumerConfig.expiredSendTimeoutMs
  private val metaStorage = (timerConsumerConfig.metaStorageCreator) ()
  private val partitionId = timerConsumerConfig.partitionId

  private val config = timerConsumerConfig.config
  private val timerTickMs = config.tickMs

  //batch related configurations
  private val maxStashTimeMs = config.batchConfig.maxStashMs
  private val maxBatchDistance = config.batchConfig.maxBatchDistance
  private val maxBatchRange = config.batchConfig.maxBatchRange
  private var disableBatchList = false
  private val enableLogSendTime = config.enableLogSendTime

  //enable dynamic batch
  private val enableDynamicBatch = config.batchConfig.dynamicBatchEnable
  private val dynamicMaxDiff = config.batchConfig.dynamicBatchMaxDiffMs

  private val stashed: mutable.ListBuffer[DelayMessageMeta] = mutable.ListBuffer.empty
  private val stashedBatches: mutable.ArrayBuffer[OffsetBatch] = mutable.ArrayBuffer.empty
  private var sendingRecords: Option[SendingRecords] = None
  private var sendingBatch: Option[Either[SendingBatch, SendingBatches]] = None
  private var stashTime: Long = SystemTime.hiResClockMs

  //parameters for memory control
  private var recordSize: Int = 1 * Size.nKB
  private val maxBatchBytes: Int = config.batchConfig.maxBatchSize

  private var currentState: State = TimerPollingState

  private var requestId: Long = 0

  private val checkInterval = FiniteDuration(timerConsumerConfig.expiredSendTimeoutMs, TimeUnit.MILLISECONDS)
  private var checkTask: Cancellable = _

  private val logTimeDiffEnable = config.logTimeDiffEnable
  private val checkMaxDiffMs = config.timerCheckMaxDiffMs

  private val baseTopic = timerConsumerConfig.baseTopic
  private val expiredTopicName = TopicMapper.getExpiredTopicName(baseTopic)

  private var lastSendTime = 0L

  private var scheduledPoll: Option[Cancellable] = None

  //处理的次序依次为： recordsToRetry -> batchToRetry -> stashedBatches -> timer

  /**
    * 所有的状态为:
    * TimerPolling: 从Timer不断地poll超时的消息，等待形成batch
    * Sending: 从stashed batches里取消息，然后发送
    * WaitingSendResult  等待发送结果
    *
    *
    * 状态变化为
    * TimerPolling -> (当形成batch以后, 发送给MessageConsumer) -> WaitingSendResult
    * TimerPolling -> (当没有新batch) -> TimerPolling
    * TimerPolling -> (当有stashed batches => 发送给 MessageConsumer) -> WaitingSendResult
    * WaitingSendResult -> (发送超时 -> 发送消息给MessageConsumer) -> WaitingSendResult
    * WaitingSendResult -> (发送成功) -> Sending
    * WaitingSendResult -> (发送失败 -> 发送消息给MessageConsumer) -> WaitingSendResult
    *
    * check的逻辑：
    * 只检查WaitingSendResult时的超时
    *
    */
  override def receive: Receive = {
    case "poll" =>
      assert(currentState == TimerPollingState)
      scheduledPoll = None
      timer.advanceClock(0)
      addMockMeta()
      if (stashedBatches.size == 0) {
        disableBatchList = false
        pollFromTimer()
      } else {
        if (stashedBatches.size == 1 || disableBatchList) {
          send(stashedBatches.remove(0))
        } else {
          send(drainBatchList(stashedBatches, maxBatchSizeHistorically))
        }
      }

    case CHECK => check()

    case RecordsSendResponse(records, sendResults, requestId) =>
      if (sendingBatch.isDefined && requestId != getRequestId(sendingBatch.get) ||
        sendingRecords.isDefined && requestId != sendingRecords.get.requestId) {
        //expired
        log.warning("Received timeout response of requestId {}, ignored", requestId)
      } else {
        if (enableLogSendTime) {
          logSendingTime()
        }
        sendingBatch.foreach { e => if (getRequestId(e) == requestId) sendingBatch = None }
        sendingRecords.foreach { e => if (e.requestId == requestId) sendingRecords = None }

        val hasFailure = sendResults.find(_.isFailure).isDefined
        if (hasFailure) {
          val success = mutable.ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
          val failed = mutable.ArrayBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
          sendResults.zip(records).foreach { e => {
            if (e._1.isFailure) {
              log.error(e._1.failed.get, "failed to send records, will retry sending")
              failed += e._2
            } else
              success += e._2
          }
          }
          confirmSuccess(success)
          send(failed)
        } else {
          confirmSuccess(records)
          currentState = TimerPollingState
          self ! "poll"
        }
      }

    case BatchConsumeFailedResponse(reason, batch, requestId) =>
      assert(currentState == WaitingSendResultState)
      if (requestId != getRequestId(sendingBatch.get)) {
        log.warning("received timeout response {}, ignored", requestId)
      } else {
        if (retryableException(reason)) {
          log.error(reason, "failed to consume, will retry, requestId: {}, batch: {}, baseTopic: {}",
            requestId, batch, baseTopic)
          send(batch)
        } else if (fatalException(reason)) {
          log.error(reason, "failed to consume, will stop current actor, requestId: {}, batch: {}, baseTopic: {}",
            requestId, batch, baseTopic)
          //will not confirm this batch, leave this to admin
          context.stop(self)
        } else {
          log.error(reason, "failed to consume, ignore this request, requestId: {}, batch: {}, baseTopic: {}",
            requestId, batch, baseTopic)
          confirm(batch)
        }
      }

    case BatchListConsumeFailedResponse(reason, batches, requestId) =>
      assert(currentState == WaitingSendResultState)
      if (requestId != getRequestId(sendingBatch.get)) {
        log.warning("Received timeout response {}, ignored", requestId)
      } else {
        if (retryableException(reason)) {
          log.error(reason, "failed to consume batches, will retry each single batch," +
            " requestId: {}, batch: {}, baseTopic: {}",
            requestId, batches, baseTopic)
          assert(stashedBatches.isEmpty)
          //retry each batch
          disableBatchList = true
          sendingBatch = None
          stashedBatches ++= batches.batches
          currentState = TimerPollingState
          self ! "poll"
        } else if (fatalException(reason)) {
          log.error(reason, "failed to consume, will stop current actor, requestId: {}, batch: {}, baseTopic: {}",
            requestId, batches, baseTopic)
          //will not confirm this batch, leave this to admin
          context.stop(self)
        } else {
          log.error(reason, "failed to consume, ignore this request, requestId: {}, batch: {}, baseTopic: {}",
            requestId, batches, baseTopic)
          batches.batches.foreach(confirm)
        }
      }

    case MessageConsumeComplete(requestId) =>
      if (sendingBatch.isDefined && getRequestId(sendingBatch.get) == requestId) {
        mayBeSendPeeked(PeekSize)
      }

    case AverageRecordSize(currentSize) =>
      recordSize = currentSize

    case PermanentlyMissedMessages(missed) =>
      confirm(missed)
  }

  private def pollFromTimer(): Unit = {
    val (dynamicBatched, gotNewMessage) = pollAvailable()
    if (dynamicBatched) {
      rollCurrentBatch()
    }
    var sentBatch = false
    if (dynamicBatched || maybeRollCurrentBatch()) {
      if (!disableBatchList) {
        val batchList = drainBatchList(stashedBatches, maxBatchSizeHistorically)
        if (!batchList.isEmpty) {
          send(batchList)
          sentBatch = true
        }
      } else {
        if (!stashedBatches.isEmpty) {
          send(stashedBatches.remove(0))
          sentBatch = true
        }
      }
    }
    if (!sentBatch) {
      //        if (gotNewMessage) {
      //          self ! "poll"
      //        } else {
      //to avoid tight loop
      scheduledPoll = Some(context.system.scheduler.scheduleOnce(100 millis, self, "poll"))
    }
    mayBeSendPeeked(PeekSize)
    //        }
  }

  //以周期性更新timer的current time，以方便cache的使用
  private def addMockMeta() = {
    timer.add(new DelayMessageMeta(-1, SystemTime.hiResClockMs + 2 * timerTickMs))
  }

  @inline
  private def maybeResetStashTime(): Unit = {
    if (stashed.isEmpty) {
      stashTime = SystemTime.hiResClockMs
    }
  }

  /**
    * @return 第一个boolean给示是否生成了dynamic batch，第二个表示是否拉取到了数据
    */
  private def pollAvailable(): (Boolean, Boolean) = {
    var continue = true
    var gotNewMessages = false
    var dynamicBatched = false
    while (continue) {
      val expired: DelayMessageMeta = expiredMessageQueue.poll()
      if (expired == null) {
        continue = false
      } else if (expired.offset != -1L) {
        gotNewMessages = true
        val now = SystemTime.hiResClockMs
        if (now - stashTime >= maxStashTimeMs) {
          continue = false
        }
        addToStashed(expired, now)
        if (enableDynamicBatch && maybeDrainDynamicBatch(expired, now)) {
          dynamicBatched = true
          continue = false
        }
      }
    }
    (dynamicBatched, gotNewMessages)
  }

  private var lastPeekTime = timer.getCurrentTime
  private val minPeekBucketNum = Math.max(1, 2048 / timerTickMs)
  private var lastPeekedBuckets = Map.empty[Long, SortedSet[DelayMessageMeta]]

  private def mayBeSendPeeked(maxNumber: Int): Unit = {
    val currentTime = timer.getCurrentTime
    if (currentTime != lastPeekTime) {
      lastPeekTime = currentTime
      //      log.debug("peek, last peek time {}", lastPeekTime)
      val peeked = timer.peek(maxNumber,
        lastPeekTime + timerTickMs + 15000,
        lastPeekTime + timerTickMs,
        minPeekBucketNum,
        lastPeekedBuckets)
//      if (!peeked.overflowed.isEmpty) {
//        val rolled = roll(sortAndRemoveDuplicated(peeked.overflowed), maxBatchDistance, maxBatchRange, maxBatchSizeHistorically)
//        messageConsumer ! PreFetchRequest(OffsetBatchList(rolled.toList), CachedTopicDelayService.PreConsumeMaxRange)
//      }
      val newlyPeekedBuffer = mutable.SortedSet.empty[DelayMessageMeta]
      peeked.buckets.foreach { bucket =>
        if (!lastPeekedBuckets.contains(bucket.bucketTime)) {
          newlyPeekedBuffer ++= bucket.messages
        } else if (lastPeekedBuckets(bucket.bucketTime).size != bucket.messages.size) {
          newlyPeekedBuffer ++= (bucket.messages -- lastPeekedBuckets(bucket.bucketTime))
        }
      }
      newlyPeekedBuffer ++= peeked.overflowed
      val rolled = roll(newlyPeekedBuffer.toList, maxBatchDistance, maxBatchRange, maxBatchSizeHistorically)
      messageConsumer ! PreFetchRequest(OffsetBatchList(rolled.toList), CachedTopicDelayService.PreConsumeMaxRange)
      lastPeekedBuckets = peeked.buckets.map { b => (b.bucketTime, b.messages) }.toMap
    }
  }

  // now是hiRes time
  private def addToStashed(meta: DelayMessageMeta, now: Long): Unit = {
    stashed += meta
    if (logTimeDiffEnable) {
      logTimeDiffMs(meta, now)
    }
  }

  /**
    * @return true表示执行了drain操作
    *         如果拉取消息时，消息已超时太久，就拉取所有当前已超时的消息，以期达到最大的批量处理效率
    */
  private def maybeDrainDynamicBatch(expired: DelayMessageMeta, now: Long): Boolean = {
    val diffMs = now - expired.expirationMsAbsolutely
    if (diffMs > dynamicMaxDiff) {
      var count = 0
      var dried = false
      while (!dried) {
        val expired = expiredMessageQueue.poll()
        if (expired == null) {
          dried = true
        } else if (expired.offset != -1L) {
          addToStashed(expired, now)
          count += 1
        }
      }
      log.info("Collected dynamic batch of size {}, diff: {}", count, diffMs)
      true
    } else {
      false
    }
  }

  /**
    * 俩都需要绝对时间
    */
  private def logTimeDiffMs(expired: DelayMessageMeta, now: Long): Unit = {
    val diff = Math.abs(expired.expirationMsAbsolutely - now)
    if (diff > checkMaxDiffMs) {
      log.warning(s"offset: ${expired.offset}, diff: $diff, expireTime: ${expired.expirationMsAbsolutely}, current $now")
    }
  }

  private def send(batchOrBatches: Either[SendingBatch, SendingBatches]): Unit = {
    batchOrBatches.fold(batch => send(batch.batch), batches => send(batches.batches))
  }


  //  private def send(batch: OffsetBatch): Unit = {
  //    lastSendTime = SystemTime.hiResClockMs
  //    val requestId = nextRequestId
  //    messageConsumer ! BatchConsumeRequest(batch, requestId)
  //    sendingBatch = Some(Left(SendingBatch(batch, SystemTime.hiResClockMs, requestId)))
  //    currentState = WaitingSendResultState
  //  }

  private def send(batch: OffsetBatch): Unit = {
    send(OffsetBatchList(List(batch)))
  }

  private def send(batches: OffsetBatchList): Unit = {
    lastSendTime = SystemTime.hiResClockMs
    val requestId = nextRequestId
    messageConsumer ! BatchListConsumeRequest(batches, requestId)
    sendingBatch = Some(Right(SendingBatches(batches, SystemTime.hiResClockMs, requestId)))
    currentState = WaitingSendResultState
  }

  private def send(batches: List[OffsetBatch]): Unit = {
    if (!batches.isEmpty) {
      if (batches.size == 1) {
        send(batches.head)
      } else {
        send(OffsetBatchList(batches))
      }
    }
  }

  private def send(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    lastSendTime = SystemTime.hiResClockMs
    val requestId = nextRequestId
    messageConsumer ! RecordsSendRequest(records, expiredTopicName, requestId)
    sendingRecords = Some(SendingRecords(records, SystemTime.hiResClockMs, requestId))
    currentState = WaitingSendResultState
  }

  private def check(): Unit = {
    if (currentState == WaitingSendResultState) {
      if (sendingBatch.isDefined) {
        if (SystemTime.hiResClockMs - getSendTime(sendingBatch.get) > requestTimeoutMs) {
          log.info("Sending batch: {} is expired, retry sending", sendingBatch.get)
          send(sendingBatch.get)
        }
      } else if (sendingRecords.isDefined) {
        if (SystemTime.hiResClockMs - sendingRecords.get.sendTime > requestTimeoutMs) {
          log.info("RecordsSendRequest: {} is expired, retry sending", sendingRecords.get)
          send(sendingRecords.get.records)
        }
      } else
        throw new IllegalStateException(s"current state $currentState, but either sendingRecords or sendingBatch is defined")
    }
  }

  private def nextRequestId = {
    requestId = requestId + 1
    requestId
  }

  private def maybeRollCurrentBatch(): Boolean = {
    if (stashed.size > 0 && (SystemTime.hiResClockMs - stashTime) > maxStashTimeMs) {
      rollCurrentBatch()
      true
    } else
      false
  }

  private def rollCurrentBatch() = {
    val batches = roll(stashed.toList, maxBatchDistance, maxBatchRange, maxBatchSizeHistorically)
    stashed.clear()
    if (stashedBatches.size != 0)
      throw new IllegalStateException("shouldn't consumer from timer until all stashed batches has been send")
    stashedBatches ++= batches
    maybeResetStashTime()
  }

  private def confirmSuccess(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    confirm(records.map(_.offset))
  }

  private def confirm(offsets: Traversable[Long]): Unit = {
    val metas = offsets.toSeq.map(offset => StoreMeta(baseTopic, partitionId, offset, null))
    metaStorage.delete(metas)
  }

  /**
    * 计算发送所花的时间
    */
  private def logSendingTime(): Unit = {
    val sendingTime = SystemTime.hiResClockMs - lastSendTime
    sendingBatch.fold()(_ match {
      case Left(batch) =>
        log.info("batch sending cost: {}, batch size: {}", sendingTime, batch.batch.size)
      case Right(batches) =>
        log.info("batches sending cost: {}, batch size: {}", sendingTime, batches.batches.batches.map(_.length).sum)
    })
  }

  private def maxBatchSizeHistorically = Math.max(maxBatchBytes / recordSize, 1)

  override def preStart(): Unit = {
    self ! "poll"
    checkTask = context.system.scheduler.schedule(checkInterval, checkInterval, self, CHECK)
    super.preStart()
  }

  override def postStop(): Unit = {
    checkTask.cancel()
    scheduledPoll.foreach(_.cancel())
    metaStorage.close()
    super.postStop()
  }
}

object TimerConsumer {

  case object CHECK


  /**
    *
    * @param stashed          被roll的消息
    * @param maxBatchDistance 相邻两个消息的offset所允许的最大差值
    * @param batchRange       (max offset in batch - min offset in batch) + 1, 并不是指实际元素的最大数量
    * @param maxBatchSize     这个batch里消息的最大数量，inclusive
    * @return
    */
  def roll(stashed: List[DelayMessageMeta],
           maxBatchDistance: Int,
           batchRange: Int,
           maxBatchSize: Int): Seq[OffsetBatch] = {
    assert(maxBatchSize > 0)
    var batchCounter = 0
    var result = mutable.ListBuffer.empty[OffsetBatch]
    val sortedStashed = sortAndRemoveDuplicated(stashed)
    val offsets = sortedStashed.map(_.offset)
    val iterator = sortedStashed.iterator
    var builderOpt: Option[OffsetBatchBuilder] = None

    def newBuilder(initOffset: Long) = {
      batchCounter = 0
      OffsetBatchBuilder.getBuilder(offsets, min = initOffset, range = batchRange, maxBatchDistance)
      //      new BitmapOffsetBatchBuilder(range = batchRange, min = initOffset, maxBatchDistance)
    }

    def add(delayMessageMeta: DelayMessageMeta): Unit = {
      if (builderOpt.isEmpty) {
        builderOpt = Some(newBuilder(delayMessageMeta.offset))
      }
      val builder = builderOpt.get
      if (batchCounter >= maxBatchSize || !builder.add(delayMessageMeta.offset)) {
        result += builder.build()
        builderOpt = Some(newBuilder(delayMessageMeta.offset))
        add(delayMessageMeta)
      }
      batchCounter += 1
    }

    while (iterator.hasNext) {
      add(iterator.next())
    }
    if (builderOpt.isDefined)
      result += builderOpt.get.build()
    result
  }

  def sortAndRemoveDuplicated(metas: List[DelayMessageMeta]): List[DelayMessageMeta] = {
    if (metas.isEmpty)
      List.empty[DelayMessageMeta]
    else {
      val sorted = metas.sortBy(_.offset)
      val result = new mutable.ArrayBuffer[DelayMessageMeta](sorted.size)
      result += sorted.head
      sorted.foldLeft(sorted.head) { (a, b) =>
        if (b.offset != a.offset)
          result += b
        b
      }
      result.toList
    }
  }

  /**
    * 需要考虑到在roll MetaBatch的时候，依据当时的bytes per message计算了每个batch的最大大小。
    * 但是，在发送间隔，bytes per message的大小可能会改变。
    * 所以，在此方法中，至少会从stashed batches里取出最早的一个batch加入到结果中。否则可能会导致程序无法前进
    *
    * @param totalSize      得到的MetaBatchList中最多包含多少个message meta
    * @param stashedBatches 暂存的MetaBatch列表。这个方法会从中取出MetaBatch
    * @return
    */
  def drainBatchList(stashedBatches: mutable.ArrayBuffer[OffsetBatch], totalSize: Int): List[OffsetBatch] = {
    if (stashedBatches.isEmpty) {
      List.empty[OffsetBatch]
    } else {
      val head = stashedBatches.remove(0)
      var result = mutable.ListBuffer(head)
      var count = head.size
      while (!stashedBatches.isEmpty && stashedBatches.head.size + count <= totalSize) {
        val next = stashedBatches.remove(0)
        result += next
        count += next.size
      }
      result.toList
    }
  }


  /**
    * @return 如果返回true，代表这个异常可以忽略并且重试。如果false，代表不能重试。
    */
  def retryableException(e: Throwable): Boolean = {
    e match {
      case _: OffsetOutOfRangeException => false
      case _: ImpossibleBatchException => false
      case _ => true
    }
  }

  def fatalException(e: Throwable): Boolean = {
    e match {
      case _: OffsetOutOfRangeException => false
      case _ => false
    }
  }

  def getRequestId(batch: Either[SendingBatch, SendingBatches]): Long = {
    batch.fold(_.requestId, _.requestId)
  }

  def getSendTime(batch: Either[SendingBatch, SendingBatches]): Long = {
    batch.fold(_.sendTime, _.sendTime)
  }

  /**
    * sendTime使用clock time
    *
    */
  case class SendingBatch(batch: OffsetBatch, sendTime: Long, requestId: Long) {
    override def toString: String = {
      s"SendingBatch(batch: $batch, requestId: $requestId)"
    }
  }

  case class SendingBatches(batches: OffsetBatchList, sendTime: Long, requestId: Long) {
    override def toString: String = {
      s"SendingBatches, requestId: $requestId, batches: $batches"
    }
  }

  case class SendingRecords(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]], sendTime: Long, requestId: Long) {
    override def toString: String = {
      s"SendingRecords(size: ${records.size}, requestId: $requestId)"
    }
  }

  val PeekSize = 256

  /**
    * 所有的状态为:
    * TimerPolling: 从Timer不断地poll超时的消息，等待形成batch
    * Sending: 从stashed batches里取消息，然后发送
    * WaitingSendResult  等待发送结果
    */
  sealed trait State

  case object TimerPollingState extends State

  case object WaitingSendResultState extends State

  case class TimerConsumerConfig(metaStorageCreator: () => MetaStorage,
                                 expiredMessageQueue: BlockingQueue[DelayMessageMeta],
                                 timer: PeekableMessageTimer,
                                 messageConsumer: ActorRef,
                                 baseTopic: String,
                                 partitionId: Int,
                                 expiredSendTimeoutMs: Long,
                                 config: DelayServiceConfig)

}