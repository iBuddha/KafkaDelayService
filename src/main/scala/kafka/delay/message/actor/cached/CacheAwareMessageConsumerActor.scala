package kafka.delay.message.actor.cached

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import kafka.delay.message.actor.request._
import kafka.delay.message.actor.TopicDelayService
import kafka.delay.message.client.MetaBasedConsumer
import kafka.delay.message.client.cache.{RecordCache, RecordSizeSampler, TempRecordCache}
import kafka.delay.message.client.parser.KeyBasedRecordExpireTimeParser
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import kafka.delay.message.utils.TopicMapper
import org.apache.kafka.clients.consumer.ConsumerRecord

import scala.collection.mutable
import scala.util.{Failure, Success, Try}

@deprecated
class CacheAwareMessageConsumerActor(baseTopic: String,
                                     partition: Int,
                                     bootstrapServers: String,
                                     expiredSender: ActorRef,
                                     cacheController: ActorRef,
                                     consumer: MetaBasedConsumer)
  extends Actor with ActorLogging {
  import CacheAwareMessageConsumerActor._

  type Record = ConsumerRecord[Array[Byte], Array[Byte]]

  private val expiredTopic: String = TopicMapper.getExpiredTopicName(baseTopic)
  //  private val recordSizeSampler: RecordSizeSampler = new RecordSizeSampler
  private var messageCount = 0L
  //  private val tp: TopicPartition = new TopicPartition(baseTopic, partition)

  //booking
  private var preConsumerRequest: Option[Either[BatchConsumeRequest, BatchListConsumeRequest]] = None
  private var preConsumeSender: Option[ActorRef] = None


  private var cacheRequestId: Long = 0L
  private val recordCacheExpireMs = TopicDelayService.RecordCacheExpireMs
  private val recordExpireMsParser = new KeyBasedRecordExpireTimeParser

  private val recordSizeSampler = new RecordSizeSampler

  private[this] val cacheActor =
    context.actorOf(
      Props(
        new RecordCacheActor(
          baseTopic,
          partition,
          bootstrapServers,
          cacheController,
          RecordCache.InitialRecordCacheSize,
          TopicDelayService.RecordCacheExpireMs,
          TopicDelayService.RecordCachePaddingMs,
          new KeyBasedRecordExpireTimeParser
        )
      ),
      s"record-cache-$baseTopic-$partition")

  //  private var preFetched: Option[PreFetchResult] = None

  override def preStart(): Unit = {
    log.debug("cache aware message consumer actor for {}#{}", baseTopic, partition)
    //    cacheController ! UpdateCacheStateRequest(consumer.getCacheMetrics().get)
    super.preStart()
  }

  override def postStop(): Unit = {
    if (consumer != null)
      consumer.close()
    super.postStop()
  }

  private val consumeReceive = waitingConsumeRequestReceive orElse proxyReceive
  private val cacheReceive = cacheConsume orElse proxyReceive

  override def receive: Receive = consumeReceive


  private def waitingConsumeRequestReceive: Receive = {
    case request@BatchConsumeRequest(batch, requestId) =>
      clearLastRequest()
      preConsumerRequest = Some(Left(request))
      preConsumeSender = Some(sender())
      onConsumeRequest(BatchListConsumeRequest(OffsetBatchList(List(batch)), requestId))

    case request: BatchListConsumeRequest =>
      clearLastRequest()
      preConsumerRequest = Some(Right(request))
      preConsumeSender = Some(sender())
      onConsumeRequest(request)
  }


  private def proxyReceive: Receive = {
    case request: PreFetchBatchListRequest =>
      cacheActor ! request

    case msg: RecordsSendRequest =>
      expiredSender forward msg

  }


  private def cacheConsume: Receive = {
    case CachedRecords(records, responseId) =>
      if (responseId != cacheRequestId) {
        log.error("received on stale cache response, this should not happen")
      } else {
        val cached = new TempRecordCache(
          System.currentTimeMillis(),
          recordExpireMsParser,
          recordCacheExpireMs,
          MaxTempCacheSize)
        records.foreach(cached +=)

        val (request, consumeRequestId) = preConsumerRequest.get.fold(
          left => (List(left.batch), left.requestId),
          right => (right.batches.batches, right.requestId)
        )

        Try {
          val batchConsumeResults = request.map(batch => consumeSingleBatch(batch, cached))
          val fetched = batchConsumeResults.flatMap(_.fetched)
          val missed = batchConsumeResults.flatMap(_.missed).flatten
          BatchConsumeResult(fetched, if (missed.isEmpty) None else Some(missed))
        } match {
          case Failure(reason) =>
            if (preConsumerRequest.get.isRight) {
              preConsumeSender.get ! BatchListConsumeFailedResponse(
                reason,
                preConsumerRequest.get.right.get.batches,
                consumeRequestId)
            } else {
              preConsumeSender.get ! BatchConsumeFailedResponse(
                reason,
                preConsumerRequest.get.left.get.batch,
                consumeRequestId)
            }
          case Success(records) =>
            onSuccess(records, consumeRequestId)
        }
      }
      context.become(consumeReceive)
  }

  private def consumeSingleBatch(batch: OffsetBatch, cached: TempRecordCache): BatchConsumeResult = {
    val consumedRecords = mutable.Map.empty[Long, Record]
    val allOffsets = mutable.Set.empty[Long]
    val toConsume = mutable.ArrayBuffer.empty[Long]
    batch.foreach(allOffsets.add)
    allOffsets.foreach { offset =>
      cached.get(offset).fold {
        toConsume += offset;
        {}
      }(r => consumedRecords.put(offset, r))
    }
    if (toConsume.isEmpty) {
      BatchConsumeResult(consumedRecords.values.toList, None)
    } else {
      val BatchConsumeResult(fetched, missed) = consumer.consume(batch, cached)
      fetched.foreach { r => consumedRecords.put(r.offset(), r) }
      BatchConsumeResult(consumedRecords.values.toList.sortBy(_.offset()), missed)
    }
  }

  private def onConsumeRequest(consumeRequest: BatchListConsumeRequest): Unit = {
    cacheRequestId = cacheRequestId + 1
    context.become(cacheReceive)
    cacheActor ! GetCached(consumeRequest.batches, cacheRequestId)
  }

  private def onSuccess(records: BatchConsumeResult, requestId: Long): Unit = {
    expiredSender.tell(RecordsSendRequest(records.fetched, expiredTopic, requestId), preConsumeSender.get)
    preConsumeSender.get ! MessageConsumeComplete(requestId)
    messageCount += records.fetched.size
    maybeUpdateRecordSize(records.fetched)
    if (records.missed.isDefined) {
      preConsumeSender.get ! PermanentlyMissedMessages(records.missed.get)
    }
  }


  private def clearLastRequest(): Unit = {
    preConsumerRequest = None
    preConsumeSender = None
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
}

object CacheAwareMessageConsumerActor {
  val CacheScaleUnit = 1024 * 1024
  val RecordSizeUpdateInterval = 10000
  val CacheScaleDownRatio = 2
  val MaxTempCacheSize = 1024 * 1024 * 10 // 10M

  case class PreFetchResult(batchList: OffsetBatchList, result: BatchConsumeResult)

}