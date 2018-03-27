package kafka.delay.message.actor.metrics

import org.apache.kafka.common.TopicPartition

/**
  * 需要考虑到：
  * 1. 缓存命中率的高低取决于：
  * * 缓存本身的大小
  * * 这个partition对应的业务本身的特点。消息超时的顺序跟消息被发送到delay topic的顺序一致，就更可能会有高的命中率
  * 2.如果给命中率越高的TopicPartition越大的缓存，有可能会使得用些cache的太小从而无法提供足够的命中率，从而无法提高自己的命中率
  * 因此，可能需要给所有cache一个最低值以防止他们饿死。
  * 3. 对于个别的partition, cache并非越大越好。因为cache内的数据的生存时间以及流量可能会维持在某个特定的水平。
  *
  */
sealed trait CacheAdjustStrategy {
  /**
    *
    * @param totalSize 总的cache大小
    * @param minSize   每个TopicPartition所用cache的最小值
    * @param metrics   此次采集到的metrics
    * @return
    */
  def compute(totalSize: Int,
              minSize: Int,
              enabledPartitions: Set[TopicPartition],
              metrics: Map[TopicPartition, RecordCacheMetrics]): Map[TopicPartition, Int]
}


/**
  * 不考虑hit ratio的影响，只按照cache总数分配
  */
class FairStrategy extends CacheAdjustStrategy {
  override def compute(totalSize: Int,
                       minSize: Int,
                       enabledPartitions: Set[TopicPartition],
                       metrics: Map[TopicPartition, RecordCacheMetrics]) = {
    val assignableSize = totalSize - minSize * enabledPartitions.size
    val extra = Math.max(assignableSize, 0) / enabledPartitions.size
    val eachSize = extra + minSize
    enabledPartitions.map { _ -> eachSize}.toMap
  }
}
