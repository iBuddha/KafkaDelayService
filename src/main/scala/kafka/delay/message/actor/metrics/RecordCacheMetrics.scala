package kafka.delay.message.actor.metrics

import kafka.utils.immutable

@immutable
case class RecordCacheMetrics(performancePoint: Double, currentCacheSize: Int, usedCacheSize: Int, version: Long)