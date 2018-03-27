package kafka.delay.message.actor.cached

import akka.actor.ActorRef
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import org.apache.kafka.clients.consumer.ConsumerRecord

object CacheRequests {

  /**
    *
    * @param batch
    * @param maxMsFromMin 从这个batch里最小的超时时间minMs至minMs + maxMsFromMin都会被作为PreConsume的结果
    * @param requestTime 请求发出的时间, 是clock time
    */
  case class PreConsume(id: Long, batch: OffsetBatch, maxMsFromMin: Long, requestTime: Long, retryCount: Int, addLocal: Boolean)
  case class RetryConsume(timerConsumer: ActorRef, batchList: OffsetBatchList, requestId: Long, retryCount: Int)
  case class AddCache(batchConsumed: List[ConsumerRecord[Array[Byte], Array[Byte]]],
                      extraConsumed: List[ConsumerRecord[Array[Byte], Array[Byte]]],
                      requestTime: Long,
                      addLocal: Boolean)
}
