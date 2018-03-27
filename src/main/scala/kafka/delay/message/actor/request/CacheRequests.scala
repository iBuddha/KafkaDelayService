package kafka.delay.message.actor.request

import akka.actor.ActorRef
import kafka.delay.message.actor.metrics.CacheController.CacheState
import kafka.delay.message.actor.metrics.RecordCacheMetrics
import kafka.delay.message.client.cache.RecordCache.CacheElement
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition

case class CacheMetricsReport(actorRef: ActorRef, tp: TopicPartition, metrics: RecordCacheMetrics)

/**
  *
  * @param size cache的大小
  * @param version cache的新版本号
  */
case class SetCacheSizeRequest(size: Int, version: Long)

case object GetCacheControllerMetaRequest
case class CacheControllerMetaResponse(maxSize: Int, miniSize: Int)

/**
  *
  * @param state 当前cache的状态
  */
case class UpdateCacheStateRequest(state: CacheState)

/**
  * controller 发给 MessageConsumerActor，请求获取它的record cache的数据
  * MessageConsumer据此检查cache的情况，可能会简单的更新自己的metrics，也可能会申请/释放cache空间
  */
case object CheckCacheStateRequest

/**
  * @param version 发出请求时，cache的版本号
  */
case class CacheSpaceRequest(tp: TopicPartition,
                             actor: ActorRef,
                             targetSize: Int,
                             performancePoint: Double,
                             version: Long)

case class CacheWarmResponse(records: List[CacheElement])
case class CacheWarmRequest(batchList: OffsetBatchList)


//用于CacheActor的消息

case class Warm(messageConsumer: ActorRef, batch: OffsetBatch, requestTime: Long)
case class CacheAdd(records: List[ConsumerRecord[Array[Byte], Array[Byte]]])
case class CacheRemove(offsets: Array[Long])
case class GetCached(offsets: OffsetBatchList, requestId: Long)
case class GetNonCachedOffsets(requiredOffsets: List[OffsetBatch])
case class NonCachedOffsets(offsets: List[OffsetBatch])
case class CachedRecords(records: List[ConsumerRecord[Array[Byte], Array[Byte]]], requestId: Long)

case class ParsedRecord(record: ConsumerRecord[Array[Byte], Array[Byte]], expireMs: Long, recordSize: Int)

case class WarmerRef(warmer: ActorRef)
case object GetWarmer