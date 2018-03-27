package kafka.delay.message.actor.cached

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import kafka.delay.message.actor.RecordCacheWarmerActor
import kafka.delay.message.actor.metrics.CacheController.CacheState
import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.actor.request._
import kafka.delay.message.client.KafkaClientCreator
import kafka.delay.message.client.cache.RecordCache._
import kafka.delay.message.client.cache.TimerBasedRecordCache
import kafka.delay.message.client.parser.RecordExpireTimeParser
import kafka.delay.message.utils.GroupNames
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.mutable

/**
  * for a single TopicPartition
  * cache records
  *
  */
@deprecated
class RecordCacheActor(baseTopic: String,
                       partition: Int,
                       bootstrapServers: String,
                       controller: ActorRef,
                       initSize: Int,
                       maxExpireMs: Long,
                       paddingTime: Long,
                       expireMsParser: RecordExpireTimeParser) extends Actor with ActorLogging {
  private[this] val cache = new TimerBasedRecordCache(initSize, maxExpireMs, paddingTime, expireMsParser)
  private[this] val baseTp = new TopicPartition(baseTopic, partition)

  private val warmerActor = context.actorOf(
    Props(
      new RecordCacheWarmerActor(bootstrapServers, baseTopic, partition, KafkaClientCreator, getProperties)
    ).withDispatcher("consumer-dispatcher"))

  override def preStart(): Unit = {
    controller ! UpdateCacheStateRequest(getCacheState)
    super.preStart()
  }

  override def receive = {
    case GetCached(batchList, requestId) =>
      val cached = mutable.ListBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      batchList.batches.foreach { batch =>
        cached ++= cache.getCached(batch).map(_.record)
      }
      log.debug("got cached {} required {}", cached.size, batchList.batches.map(_.size).sum)
      sender ! CachedRecords(cached.toList, requestId)

    case CacheAdd(records) =>
      val now = System.currentTimeMillis()
      records.foreach { r => cache += (r, now) }

    case CacheRemove(offsets) =>
      offsets.foreach(cache -= _)

    case SetCacheSizeRequest(size, version) =>
      log.debug("set cache size to {}", size)
      cache.resize(size, System.currentTimeMillis(), version)

    case CheckCacheStateRequest =>
      updateState()

    case r: PreFetchBatchListRequest =>
      warmerActor ! r

    case GetNonCachedOffsets(batches) =>
      val nonCached = batches.map { batch =>
        cache.getNonCachedOffsets(batch)
      }
      sender ! NonCachedOffsets(nonCached)
  }

  private def updateState() {
    log.debug("checking cache state")
    val metrics = cache.metrics()
    log.debug("sending cache metrics {}", metrics)
    val requestedScale = maybeScaleCache(metrics)
    if (!requestedScale) {
      sender ! UpdateCacheStateRequest(toState(metrics))
    }
  }

  @inline
  private def needMoreSpace: Boolean = {
    val metrics = cache.metrics()
    metrics.currentCacheSize <= 0 || metrics.usedCacheSize.toDouble / metrics.currentCacheSize > 0.9
  }

  /**
    * 是否发送了scale cache的请求
    *
    * @param metrics
    * @return
    */
  @inline
  private def maybeScaleCache(metrics: RecordCacheMetrics): Boolean = {
    if (needMoreSpace) {
      requestCacheSpace(CacheScaleUnit, metrics)
      log.debug("request new space, current metrics: {}", metrics)
      true
    } else if (needScaleDownCache(metrics)) {
      log.debug("request less space, current metrics: {}", metrics)
      requestCacheSpace(-CacheScaleUnit, metrics)
      true
    } else
      false
  }

  @inline
  private def needScaleDownCache(metrics: RecordCacheMetrics): Boolean = {
    metrics.currentCacheSize >= CacheScaleUnit &&
      metrics.currentCacheSize / Math.max(1, metrics.usedCacheSize) >= CacheScaleDownRatio
  }

  @inline
  private def requestCacheSpace(diff: Int, metrics: RecordCacheMetrics): Unit = {
    controller.tell(
      CacheSpaceRequest(
        baseTp,
        self,
        Math.max(metrics.currentCacheSize + diff, 0),
        metrics.performancePoint,
        metrics.version),
      self)
  }

  private def toState(metrics: RecordCacheMetrics): CacheState = {
    CacheState(metrics.version, baseTp, self, metrics.currentCacheSize, metrics.usedCacheSize, Some(metrics.performancePoint))
  }

  //case class CacheState(version: Long, tp: TopicPartition, actor: ActorRef, currentSize: Int, point: Option[Double])

  private def getCacheState: CacheState = {
    val metrics = cache.metrics()
    CacheState(metrics.version, baseTp, self, metrics.currentCacheSize, metrics.usedCacheSize, Some(metrics.performancePoint))
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

  //  @inline
  //  private def updateState(): Unit = {
  //    log.debug("checking cache state")
  //    val metrics = cache.getCacheMetrics().get
  //    log.debug("sending cache metrics {}", metrics)
  //    val requestScale = maybeScaleCache(metrics)
  //    if (!requestScale) {
  //      sender ! UpdateCacheStateRequest(metrics)
  //    }
  //  }
}
