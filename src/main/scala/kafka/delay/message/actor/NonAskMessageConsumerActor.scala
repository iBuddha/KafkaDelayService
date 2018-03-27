package kafka.delay.message.actor

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import kafka.delay.message.actor.NonAskMessageConsumerActor._
import kafka.delay.message.actor.metrics.CacheController.CacheState
import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.actor.request._
import kafka.delay.message.client.{KafkaClientCreator, MetaBasedConsumer}
import kafka.delay.message.client.cache.{RecordCache, RecordSizeSampler}
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import kafka.delay.message.utils.{GroupNames, SystemTime, TopicMapper}
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.util.{Failure, Success, Try}

/**
  * 与MessageConsumerActor不同，这个actor不使用ask的模式，而采用tell，以减少阻塞的开销以及排除死锁的可能性
  *
  * TODO: 处理batch list时，并行化
  */
class NonAskMessageConsumerActor(baseTopic: String,
                                 partition: Int,
                                 bootstrapServers: String,
                                 expiredSender: ActorRef,
                                 cacheController: Option[ActorRef],
                                 consumer: MetaBasedConsumer)
  extends Actor with ActorLogging {

  private val expiredTopic: String = TopicMapper.getExpiredTopicName(baseTopic)
  private val recordSizeSampler: RecordSizeSampler = new RecordSizeSampler
  private var messageCount = 0L
  private val tp: TopicPartition = new TopicPartition(baseTopic, partition)
  private[this] val cacheEnabled = cacheController.isDefined
  //  private var preFetched: Option[PreFetchResult] = None
  private var cacheWarmerActor: Option[ActorRef] = None

  override def preStart(): Unit = {
    log.debug("cache enabled : {}, init metrics {}", cacheEnabled, consumer.getCacheMetrics())
    if (cacheEnabled) {
      cacheController.get ! UpdateCacheStateRequest(consumer.getCacheMetrics().get)
      cacheWarmerActor = Some(
        context.actorOf(
          Props(
            new RecordCacheWarmerActor(bootstrapServers, baseTopic, partition, KafkaClientCreator, getProperties)
          )
        )
      )
    }
    super.preStart()
  }

  override def postStop(): Unit = {
    if (consumer != null)
      consumer.close()
    super.postStop()
  }

  override def receive: Receive = {
    case BatchConsumeRequest(batch, requestId) =>
      Try {
        consume(batch)
        //        consumer.consume(batch)
      } match {
        case Failure(reason) =>
          sender ! BatchConsumeFailedResponse(reason, batch, requestId)
        case Success(records) =>
          onSuccess(records, requestId)
      }

    case BatchListConsumeRequest(batches, requestId) =>
      Try {
        consume(batches)
      } match {
        case Failure(reason) =>
          sender ! BatchListConsumeFailedResponse(reason, batches, requestId)
        case Success(records) =>
          onSuccess(records, requestId)
      }

    case GetWarmer =>
      sender ! WarmerRef(cacheWarmerActor.get)

    case CacheAdd(fetched) =>
      consumer.addToCache(fetched)

    case msg: RecordsSendRequest =>
      expiredSender forward msg

    case SetCacheSizeRequest(size, version) =>
      log.debug("set cache size to {}", size)
      consumer.resetCacheSize(size, version)

    case CheckCacheStateRequest =>
      updateState()

  }

  private def consume(batch: OffsetBatch) = {
    if (log.isDebugEnabled) {
      val startTime = SystemTime.hiResClockMs
      val result = consumer.consume(batch)
      val endTime = SystemTime.hiResClockMs
      log.debug("consume cost {}", endTime - startTime)
      result
    } else
      consumer.consume(batch)
  }

  private def consume(batches: OffsetBatchList) = {
    if(log.isDebugEnabled) {
      val startTime = SystemTime.hiResClockMs
      val result = consumer.consume(batches)
      val endTime = SystemTime.hiResClockMs
      log.debug("consume cost {}", endTime - startTime)
      result
    }else {
      consumer.consume(batches)
    }
  }

  private def onSuccess(records: BatchConsumeResult, requestId: Long): Unit = {
    expiredSender forward RecordsSendRequest(records.fetched, expiredTopic, requestId)
    sender() ! MessageConsumeComplete(requestId)
    messageCount += records.fetched.size
    maybeUpdateRecordSize(records.fetched)
//    maybeScaleCache()
    if (records.missed.isDefined) {
      sender ! PermanentlyMissedMessages(records.missed.get)
    }
  }

  @inline
  private def updateState(): Unit = {
      if (cacheController.isDefined) {
        cacheController.get ! UpdateCacheStateRequest(consumer.getCacheMetrics().get)
      }
      maybeScaleCache()
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
    if (cacheEnabled) {
      val metrics = consumer.getCacheMetrics().get
      if (needScaleUpCache(metrics)) {
        requestCacheSpace(RecordCache.CacheScaleUnit, metrics)
        true
      } else if (needScaleDownCache(metrics)) {
        requestCacheSpace(-RecordCache.CacheScaleUnit, metrics)
        true
      } else
        false
    } else
      false
  }

  @inline
  private def needScaleDownCache(metrics: RecordCacheMetrics): Boolean = {
    consumer.needMoreCacheSpace || (
    metrics.currentCacheSize > RecordCache.CacheScaleUnit &&
      metrics.currentCacheSize / Math.max(1, metrics.usedCacheSize) >= RecordCache.CacheScaleDownRatio
      )
  }

  @inline
  private def needScaleUpCache(metrics: RecordCacheMetrics): Boolean = {
    metrics.currentSize > 0 && metrics.usedCacheSize.toDouble / metrics.currentCacheSize > scaleUpRatio
  }

  @inline
  private def requestCacheSpace(diff: Int, metrics: RecordCacheMetrics): Unit = {
    cacheController.foreach { cache =>
      cache.tell(
        CacheSpaceRequest(
          tp,
          self,
          Math.max(metrics.currentCacheSize + diff, metrics.usedCacheSize),
          metrics.performancePoint,
          metrics.version),
        self)
    }
  }

  implicit private def toState(metrics: RecordCacheMetrics): CacheState = {
    CacheState(metrics.version,
      tp,
      self,
      currentSize = metrics.currentCacheSize,
      usedSize = metrics.usedCacheSize,
      Some(metrics.performancePoint))
  }

  private def getProperties: Properties = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.cacheWarmerConsumerGroup(baseTopic))
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") //reset可以导致poll消息的offset不是连续的
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config
  }
}

object NonAskMessageConsumerActor {
  val RecordSizeUpdateInterval = 10000
  val scaleUpRatio = 0.9
  case class PreFetchResult(batchList: OffsetBatchList, result: BatchConsumeResult)

}