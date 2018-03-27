package kafka.delay.message.actor.cached

import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern.{Backoff, BackoffSupervisor}
import akka.routing._
import kafka.delay.message.actor.Dispatchers
import kafka.delay.message.actor.cached.CacheRequests.{AddCache, PreConsume, RetryConsume}
import kafka.delay.message.actor.metrics.CacheController.CacheState
import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.actor.request._
import kafka.delay.message.client.cache.RecordCache.CachedAndNot
import kafka.delay.message.client.cache.{RecordCache, RecordSizeSampler, TimerBasedRecordCache}
import kafka.delay.message.client.parser.KeyBasedRecordExpireTimeParser
import kafka.delay.message.timer.meta.{ArrayOffsetBatch, OffsetBatch, OffsetBatchList}
import kafka.delay.message.utils.{SystemTime, Time, TopicMapper}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

class CachedMessageConsumerActor(baseTopic: String,
                                 partition: Int,
                                 bootstrapServers: String,
                                 expiredSender: ActorRef,
                                 cacheController: ActorRef,
                                 cache: TimerBasedRecordCache,
                                 consumer: NoCacheMessageConsumer)
  extends Actor with ActorLogging {

  import CachedMessageConsumerActor._

  private val expiredTopic: String = TopicMapper.getExpiredTopicName(baseTopic)
  private val recordSizeSampler: RecordSizeSampler = new RecordSizeSampler
  private var messageCount = 0L
  private val tp: TopicPartition = new TopicPartition(baseTopic, partition)
  private val workerNumber = 15
  private var workerRouter: Option[Router] = None
  private var needMoreCacheSpace = false
  private var lastCacheSpaceRequestTime = -1L
  private implicit val executionContext = context.dispatcher
  private val scheduler = context.system.scheduler
  private var preConsumeId = 0L
  private val localCachedRecords: mutable.Map[Long, ConsumerRecord[Array[Byte], Array[Byte]]] = mutable.Map.empty


  override def preStart(): Unit = {
    cacheController ! UpdateCacheStateRequest(cache.metrics())

    //    var router = {
    //      val routees = Vector.fill(5) {
    //        val r = context.actorOf(Props[Worker])
    //        context watch r
    //        ActorRefRoutee(r)
    //      }
    //      Router(RoundRobinRoutingLogic(), routees)
    //    }
    val workers = (0 until workerNumber).map { i =>
      val worker = createWorker("worker_" + i.toString)
      context.watch(worker)
      worker
    }
    val routees = workers.map(ActorRefRoutee(_): Routee)
//        workerRouter = Some(Router(TailChoppingRoutingLogic, routees))
    workerRouter = Some(Router(RoundRobinRoutingLogic(), routees))
    super.preStart()
  }

  override def postStop(): Unit = {
    if (consumer != null)
      consumer.close()
    super.postStop()
  }

  override def receive: Receive = {

    case BatchListConsumeRequest(batches, requestId) =>
      onConsumeRequestWithCache(sender(), batches, requestId, 0)

    case RetryConsume(timerConsumer, batches, requestId, retryCount) =>
      onConsumeRequestWithCache(timerConsumer, batches, requestId, retryCount)

    case AddCache(batchConsumed, extraConsumed, requestTime, addLocal) =>
      if (addLocal) {
        //TODO: 只需实际需要的才可以放在local cache里，不然可能会占用太多内存
        batchConsumed.foreach { r => localCachedRecords.put(r.offset(), r) }
      } else {
        addToCache(batchConsumed, extraConsumed)
        if (log.isDebugEnabled) {
          val now = SystemTime.milliseconds
          val cost = now - requestTime
          log.debug("delayed {} adding {} records to cache, size after add is {}",
            cost,
            batchConsumed.size + extraConsumed.size,
            cache.recordCount)
        }
      }

    case msg: RecordsSendRequest =>
      expiredSender forward msg

    case SetCacheSizeRequest(size, version) =>
      log.debug("set cache size to {}", size)
      needMoreCacheSpace = false
      cache.resize(size, SystemTime.milliseconds, version)

    case CheckCacheStateRequest =>
      updateState()

    //TODO: stash pre-consume。因为在一个PreWarm请求在worker处排队的过程中，它之前的PreWarm可能已经读取了它将要读取的数据
    case PreFetchRequest(batches, maxExpireMs) => {
      val nonCached = batches.batches.map { batch =>
        cache.getNonCachedOffsets(batch)
      }.filterNot(_.isEmpty)
      if (log.isDebugEnabled && !nonCached.isEmpty) {
        log.debug("got prefetch request with batch number {}, non-cached batch number {}",
          batches.batches.size,
          nonCached.size)
      }
      val now = System.currentTimeMillis()
      nonCached.foreach { batch =>
        preConsumeId = preConsumeId + 1
        workerRouter.get.route(PreConsume(preConsumeId, batch, maxExpireMs, now, 0, false), self)
      }
    }

    case r@PreConsume(id, batch, maxMsFromMini, requestTime, retryCount, addLocal) =>
      val nonCached = cache.getNonCachedOffsets(batch)
      if (!nonCached.isEmpty) {
        workerRouter.get.route(r.copy(retryCount = retryCount + 1), self)
        if (retryCount <= 2) {
          workerRouter.get.route(r.copy(retryCount = retryCount + 1), self)
        }
      } else if (log.isDebugEnabled) {
        val now = SystemTime.milliseconds
        log.debug("PreConsume {} with {} records succeeded after retry {}ms", id, batch.size, now - requestTime)
      }

    case Terminated(worker) => {
      log.warning(worker + "terminated")
      workerRouter.get.removeRoutee(worker)
      val newWorker = createWorker(worker.path.name)
      workerRouter = Some(workerRouter.get.addRoutee(newWorker))
    }
  }


  //  private def onConsumeRequest(batches: OffsetBatchList, requestId: Long) = {
  //    Try {
  //      consume(batches.batches)
  //    } match {
  //      case Failure(reason) =>
  //        sender ! BatchListConsumeFailedResponse(reason, batches, requestId)
  //      case Success(records) =>
  //        onSuccess(records, requestId)
  //    }
  //  }

  //  private def onConsumeRequest(timerConsumer: ActorRef, batches: OffsetBatchList, requestId: Long, retryCount: Int) = {
  //    val testResults = batches.batches.map(cache.test)
  //    val nonCached = testResults.flatMap(_.nonCached)
  //    if (!nonCached.isEmpty && retryCount <= MaxRetry) {
  //      log.info("retry consume with {} non-cached message, retry count {}", nonCached.map(_.size).sum, retryCount)
  //      if (retryCount == 0) {
  //        nonCached.foreach { batch =>
  //          preConsumeId += 1
  //          workerRouter.get.route(PreConsume(preConsumeId, batch, 0, SystemTime.milliseconds, 0, true), self)
  //          workerRouter.get.route(PreConsume(preConsumeId, batch, 0, SystemTime.milliseconds, 0, true), self) //speculative consuming
  //        }
  //      }
  //      //TODO: 这里无法确定retry之后结果一定就在cache里，因为retry得到的结果可能不符合进入cache的条件，所以需要另一个cache
  //      scheduler.scheduleOnce(
  //        RetryConsumeDelay,
  //        self,
  //        RetryConsume(timerConsumer, batches, requestId, retryCount + 1)
  //      )
  //    } else {
  //      val cached = testResults.flatMap(_.cached)
  //      //      cached.foreach { r =>
  //      //        cache -= r.offset() //TODO: 这里可能并不需要减去，因为会周期性地移除过期的offset
  //      //      }
  //      consume(timerConsumer, batches, nonCached, cached, requestId)
  //    }
  //  }

  private def onConsumeRequestWithCache(timerConsumer: ActorRef, batches: OffsetBatchList, requestId: Long, retryCount: Int) = {
    if (retryCount == 0) {
      val testResults = batches.batches.map(cache.test)
      val nonCached = testResults.flatMap(_.nonCached)
      if (!nonCached.isEmpty) {
        //防止在重试过程中，cache里的数据被移除
        testResults.foreach { result =>
          result.cached.foreach { r =>
            localCachedRecords.put(r.offset(), r)
            cache -= r.offset()
          }
        }
        val preConsumeIds = new ArrayBuffer[Long]
        nonCached.foreach { batch =>
          preConsumeId += 1
          workerRouter.get.route(PreConsume(preConsumeId, batch, 0, SystemTime.milliseconds, 0, true), self)
          workerRouter.get.route(PreConsume(preConsumeId, batch, 0, SystemTime.milliseconds, 0, true), self) //speculative consuming
          preConsumeIds += preConsumeId
        }
        log.info("retry consume with {} non-cached message, retry count {}, preConsume ids {}, requestId {}",
          nonCached.map(_.size).sum,
          retryCount,
          preConsumeIds,
          requestId)
        //TODO: 这里无法确定retry之后结果一定就在cache里，因为retry得到的结果可能不符合进入cache的条件，所以需要另一个cache
        scheduler.scheduleOnce(
          RetryConsumeDelay,
          self,
          RetryConsume(timerConsumer, batches, requestId, retryCount + 1)
        )
      } else {
        consume(timerConsumer, batches, nonCached, testResults.flatMap(_.cached), requestId)
      }
    } else if (retryCount < MaxRetry) {
      //try with local cache
      val localTestResult = batches.batches.map(testLocalCache)
      val localNonCached = localTestResult.flatMap(_.nonCached)
      log.info("got retry consume with {} non-cached message, retry count {}, requestId {}",
        localNonCached.map(_.size).sum,
        retryCount,
        requestId)
      if (localNonCached.isEmpty) {
        val cached = localTestResult.flatMap(_.cached)
        consume(timerConsumer, batches, List.empty, cached, requestId)
      } else {
        scheduler.scheduleOnce(
          RetryConsumeDelay,
          self,
          RetryConsume(timerConsumer, batches, requestId, retryCount + 1)
        )
      }
    } else {
      val localTestResult = batches.batches.map(testLocalCache)
      val localNonCached = localTestResult.flatMap(_.nonCached)
      val cached = localTestResult.flatMap(_.cached)
      log.info("got retry consume with {} non-cached message, retry count {}, requestId {}",
        localNonCached.map(_.size).sum,
        retryCount,
        requestId)
      consume(timerConsumer, batches, localNonCached, cached, requestId)
    }
  }

  private def testLocalCache(batch: OffsetBatch): CachedAndNot = {
    val cached = mutable.ListBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
    val nonCached = mutable.ArrayBuffer.empty[Long]
    batch.foreach { offset =>
      val cachedRecord = localCachedRecords.get(offset)
      if (cachedRecord.isDefined) {
        cached += cachedRecord.get
      } else {
        nonCached += offset
      }
    }
    CachedAndNot(cached.toList, if (!nonCached.isEmpty) Some(new ArrayOffsetBatch(nonCached.toArray)) else None)
  }


  private def consume(timerConsumer: ActorRef,
                      originalBatchList: OffsetBatchList,
                      nonCachedBatches: List[OffsetBatch],
                      cachedRecords: List[ConsumerRecord[Array[Byte], Array[Byte]]],
                      requestId: Long) = {
    Try {
      localCachedRecords.clear()
      cachedRecords.foreach(cache -= _.offset())
      if (nonCachedBatches.isEmpty) {
        BatchConsumeResult(cachedRecords, None)
      } else {
        val consumed = forceConsume(nonCachedBatches)
        val fetched = consumed.fetched ++ cachedRecords
        BatchConsumeResult(fetched, consumed.missed)
      }
    } match {
      case Failure(reason) =>
        timerConsumer ! BatchListConsumeFailedResponse(reason, originalBatchList, requestId)
      case Success(records) =>
        onSuccess(timerConsumer, records, requestId)
    }
  }

  /**
    * 加一些记录到cache
    * T
    *
    */
  private def addToCache(batchConsumed: List[ConsumerRecord[Array[Byte], Array[Byte]]],
                         extraConsumed: List[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    val now = SystemTime.milliseconds
    var needMoreSpace = false
    batchConsumed.foreach { record =>
      if (cache += (record, now)) {
        needMoreSpace = true
      }
    }
    if (needMoreSpace && SystemTime.milliseconds - lastCacheSpaceRequestTime > MaxCacheScaleIntervalMs) {
      log.debug("need more cache space with metrics {}", cache.metrics())
      needMoreCacheSpace = true
      maybeScaleCache()
    }
    if(!needMoreSpace){
      var continue = true
      val iterator = extraConsumed.iterator
      while(iterator.hasNext && continue){
        continue = !(cache.tryAdd(iterator.next, now))
      }
    }
    if (!needMoreSpace)
      needMoreCacheSpace = false
  }

  @inline
  private def updateState(): Unit = {
    cache.removeExpiredBefore(System.currentTimeMillis() - RecordCache.ResizePaddingMs)
    cacheController ! UpdateCacheStateRequest(cache.metrics())
    maybeScaleCache()
  }

  //  /**
  //    *
  //    * @param batch 非空
  //    * @return
  //    */
  //  private def consume(batch: OffsetBatch): BatchConsumeResult = {
  //    val cachedAndNot = cache.get(batch)
  //    if (cachedAndNot.nonCached.isDefined) {
  //      log.debug("cached: {}, non-cached message: {}", cachedAndNot.cached.size, cachedAndNot.nonCached.size)
  //    }
  //    if (cachedAndNot.nonCached.isEmpty) {
  //      BatchConsumeResult(cachedAndNot.cached.get, None)
  //    } else {
  //      val nonCached = cachedAndNot.nonCached.get
  //      val cached = cachedAndNot.cached.getOrElse(List.empty)
  //      val fetched = consumer.consume(nonCached)
  //      assert(cached.size + fetched.missed.fold(0)(_.size) + fetched.fetched.size == batch.size)
  //      BatchConsumeResult(fetched.fetched ++ cached, fetched.missed)
  //    }
  //  }


  //  private def consume(batches: List[OffsetBatch]): BatchConsumeResult = {
  //    val consumeResult = batches.map(consume)
  //    val consumed = consumeResult.flatMap(_.fetched)
  //    val missed = consumeResult.flatMap(_.missed.getOrElse(Traversable.empty))
  //    BatchConsumeResult(consumed, if (missed.isEmpty) None else Some(missed))
  //  }

  private def forceConsume(batches: List[OffsetBatch]): BatchConsumeResult = {
    val consumeBegin = SystemTime.hiResClockMs
    val consumeResult = batches.map(consumer.consume)
    val consumeEnd = SystemTime.hiResClockMs
    val consumed = consumeResult.flatMap(_.fetched)
    val missed = consumeResult.flatMap(_.missed.getOrElse(Traversable.empty))
    log.info("message consumer forced to consume {} messages {} cost {}",
      batches.map(_.size).sum,
      batches.flatten,
      consumeEnd - consumeBegin)
    BatchConsumeResult(consumed, if (missed.isEmpty) None else Some(missed))
  }

  private def onSuccess(timerConsumer: ActorRef, records: BatchConsumeResult, requestId: Long): Unit = {
    expiredSender.tell(RecordsSendRequest(records.fetched, expiredTopic, requestId), timerConsumer)
    timerConsumer ! MessageConsumeComplete(requestId)
    messageCount += records.fetched.size
    maybeUpdateRecordSize(records.fetched)
    //    maybeScaleCache()
    if (records.missed.isDefined) {
      timerConsumer ! PermanentlyMissedMessages(records.missed.get)
    }
  }

  @inline
  private def maybeUpdateRecordSize(records: Seq[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    if (messageCount >= RecordSizeUpdateInterval) {
      messageCount = 0
      records.foreach(recordSizeSampler +=)
      val currentSize = recordSizeSampler.average.toInt
      sender ! AverageRecordSize(currentSize)
    }
  }


  /**
    * 是否发送了scale cache的请求
    *
    * @return
    */
  @inline
  private def maybeScaleCache(): Boolean = {
    val metrics = cache.metrics()
    if (needScaleUpCache(metrics)) {
      requestCacheSpace(RecordCache.CacheScaleUnit, metrics)
      true
    } else if (needScaleDownCache(metrics)) {
      requestCacheSpace(-RecordCache.CacheScaleUnit, metrics)
      true
    } else
      false
  }

  @inline
  private def needScaleDownCache(metrics: RecordCacheMetrics): Boolean = {
    metrics.currentCacheSize > RecordCache.CacheScaleUnit &&
      metrics.currentCacheSize / Math.max(1, metrics.usedCacheSize) >= RecordCache.CacheScaleDownRatio &&
      (metrics.usedCacheSize.toDouble / (metrics.currentSize - RecordCache.CacheScaleUnit) < RecordCache.ScaleUpRatio)
  }

  @inline
  private def needScaleUpCache(metrics: RecordCacheMetrics): Boolean = {
    needMoreCacheSpace || (metrics.currentSize > 0 && metrics.usedCacheSize.toDouble / metrics.currentCacheSize > scaleUpRatio)
  }

  @inline
  private def requestCacheSpace(diff: Int, metrics: RecordCacheMetrics): Unit = {
    lastCacheSpaceRequestTime = SystemTime.milliseconds
    val request = CacheSpaceRequest(
      tp,
      self,
      Math.max(metrics.currentCacheSize + diff, metrics.usedCacheSize),
      metrics.performancePoint,
      metrics.version)
    log.debug("request cache space {}, current size {}, used size {}",
      request.targetSize,
      metrics.currentSize,
      metrics.usedCacheSize)
    cacheController.tell(request, self)
  }

  implicit private def toState(metrics: RecordCacheMetrics): CacheState = {
    CacheState(metrics.version,
      tp,
      self,
      metrics.currentCacheSize,
      metrics.usedCacheSize,
      Some(metrics.performancePoint))
  }

  private def createWorker(name: String): ActorRef = {
    context.actorOf(
      getWorkerSupervisorProps(
        Props(new MessageConsumerWorker(self, bootstrapServers, baseTopic, partition, new KeyBasedRecordExpireTimeParser))
          .withDispatcher(Dispatchers.WorkerConsumerDispathcer),
        name),
      "supervisor-" + name
    )
  }
}

object CachedMessageConsumerActor {
  val RecordSizeUpdateInterval = 10000
  val scaleUpRatio = 0.9
  val MaxRetry = 2
  val RetryConsumeDelay = FiniteDuration(128, TimeUnit.MILLISECONDS)
  val MaxCacheScaleIntervalMs = 10 * Time.MsPerSec

  case class PreFetchResult(batchList: OffsetBatchList, result: BatchConsumeResult)

  def getWorkerSupervisorProps(childProps: Props, actorName: String): Props =
    BackoffSupervisor.props(
      Backoff.onFailure(
        childProps,
        childName = actorName,
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
      )).withDispatcher(Dispatchers.WorkerConsumerDispathcer)
}
