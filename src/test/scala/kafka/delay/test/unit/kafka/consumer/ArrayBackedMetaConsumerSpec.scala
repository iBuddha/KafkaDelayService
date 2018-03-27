package kafka.delay.test.unit.kafka.consumer

import java.util

import kafka.delay.message.storage.AbstractKafkaMetaStorage.MetaKey
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FlatSpec, Matchers}

class ArrayBackedMetaConsumerSpec extends FlatSpec with Matchers {
  "ArrayBackedMetaConsumer" should "work correctly" in {
    val consumer = new ArrayBackedMetaConsumer("test", 3, OffsetResetStrategy.LATEST, 2)
    val partitions = util.Arrays.asList((0 until 3).map(new TopicPartition("test", _)): _*)
    consumer.assign(partitions)
    consumer.add(0, MetaKey(0, 0), 1L)
    consumer.add(1, MetaKey(0, 0), 1L)
    var records = consumer.poll(100)
    assert(records.count() == 0)
    consumer.add(0, consumer.randomKey(), 1L)
    consumer.add(1, consumer.randomKey(), 1L)
    records = consumer.poll(100)
    assert(records.count() == 2)
    consumer.addRandom(0, 3)
    consumer.addRandom(1, 3)
    consumer.addRandom(2, 3)
    records = consumer.poll(100)
    assert(records.records(new TopicPartition("test", 0)).size() == 2)
    assert(records.records(new TopicPartition("test", 1)).size() == 2)
    assert(records.records(new TopicPartition("test", 2)).size() == 2)
    consumer.unsubscribe()
    records = consumer.poll(100)
    assert(records.count == 0)
    consumer.assign(partitions)
    consumer.seekToBeginning(partitions)
    records = consumer.poll(100)
    assert(records.count == 6)
    consumer.seekToEnd(partitions)
    records = consumer.poll(100)
    assert(records.count == 0)
  }

}
