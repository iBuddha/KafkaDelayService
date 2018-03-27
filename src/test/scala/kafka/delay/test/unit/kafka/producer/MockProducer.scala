package kafka.delay.test.unit.kafka.producer

import java.util
import java.util.concurrent.{Future, TimeUnit}

import org.apache.kafka.clients.producer.{Callback, Producer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo}

class MockProducer[K,V] extends Producer[K, V]{
  override def flush(): Unit = {}

  override def partitionsFor(s: String): util.List[PartitionInfo] = null

  override def metrics(): util.Map[MetricName, _ <: Metric] = null

  override def close(): Unit = {}

  override def close(l: Long, timeUnit: TimeUnit): Unit = {}

  override def send(producerRecord: ProducerRecord[K, V]): Future[RecordMetadata] = null

  override def send(producerRecord: ProducerRecord[K, V], callback: Callback): Future[RecordMetadata] = null
}
