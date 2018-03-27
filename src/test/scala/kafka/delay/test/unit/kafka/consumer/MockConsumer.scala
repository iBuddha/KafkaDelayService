package kafka.delay.test.unit.kafka.consumer

import java.util

import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition


/**
  * 因为scalamock在mock Java接口时会有问题，所以需要搞个Scala类来替换
  */
trait MockConsumer[K, V] extends Consumer[K, V]{
  override def poll(timeout: Long): ConsumerRecords[K, V] = null

  override def subscribe(topics: util.Collection[String]): Unit = {}

  override def seek(partition: TopicPartition, offset: Long): Unit = {}

  override def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {}
}
