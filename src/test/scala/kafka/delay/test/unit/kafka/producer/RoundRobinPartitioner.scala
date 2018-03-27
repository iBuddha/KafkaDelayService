package kafka.delay.test.unit.kafka.producer

import java.util
import java.util.concurrent.atomic.AtomicLong

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster

class RoundRobinPartitioner extends Partitioner {
  var count = new AtomicLong(0)

  override def partition(topic: String, key: scala.Any, keyBytes: Array[Byte], value: scala.Any, valueBytes: Array[Byte], cluster: Cluster): Int = {
    (count.getAndIncrement() % cluster.availablePartitionsForTopic(topic).size()).toInt
  }

  override def close(): Unit = {}

  override def configure(configs: util.Map[String, _]): Unit = {}
}