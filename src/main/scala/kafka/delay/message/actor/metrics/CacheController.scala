package kafka.delay.message.actor.metrics

import akka.actor.ActorRef
import kafka.delay.message.actor.metrics.CacheController.CacheState
import org.apache.kafka.common.TopicPartition

trait CacheController {

  /**
    *
    * @param version 依据哪个cache的状态版本做出的决定
    * @param tp 提出申请的cache的TopicPartition
    * @param requestSize 想要额外申请的内存数量，以byte计
    * @param performancePoint cache的效用评分。所有cache都用同样的方法评估。只可用于排序，此值的绝对值的比例关系并不对应实际效用的比较关系。
    *                         (总的来说，就像是评分体系，比如1星至5星，并不意味着5星的效率是1星的5倍)
    * @return 需要调整大小的cache在调整后的状态
    */
  def assign(version: Long,
             tp: TopicPartition,
             actor: ActorRef,
             requestSize: Int,
             performancePoint: Double):Map[TopicPartition, CacheState]

  /**
    * 更新一个cache的metrics
    */
  def update(state: CacheState): Unit

  def getState(tp: TopicPartition): Option[CacheState]

  def getCaches(): List[CacheState]

  def currentCaches(): Map[TopicPartition, CacheState]

  def usedSize: Int

  def actorOf(tp: TopicPartition): Option[ActorRef]
}

object CacheController {

  /**
    * @param version cache的版本号，这个版本号在cache size变更的时候加1，在performance point变更的时候不变
    * @param currentSize
    * @param point 如果为None, 表示尚未存在有意义的值
    */
  case class CacheState(version: Long,
                        tp: TopicPartition,
                        actor: ActorRef,
                        currentSize: Int,
                        usedSize: Int,
                        point: Option[Double])
}

