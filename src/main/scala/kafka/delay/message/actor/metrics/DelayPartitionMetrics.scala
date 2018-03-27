package kafka.delay.message.actor.metrics

import kafka.utils.immutable

//case class DelayPartitionMetric(topic: String,
//                                partition: Int,
//                                cacheHitRatio: Double,
//                                currentCacheSize: Int,
//                                messagePerSec: Int,
//                                sequenceRatio: Double)

/**
  *
  * @param topic
  * @param partition
  */
@immutable
case class DelayPartitionMetrics(topic: String,
                                 partition: Int,
                                 cacheMetrics: RecordCacheMetrics
                               )


