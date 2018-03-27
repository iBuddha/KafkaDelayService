package kafka.delay.message.client

import java.util
import java.util.Map

import kafka.delay.message.utils.{SystemTime, TopicMapper}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecords, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * 在使用seek之前，必须先调用ensureAssignment
  *
  * @param baseTopic to consume from
  */
class MetaConsumer(baseTopic: String,
                   consumer: Consumer[Array[Byte], Array[Byte]]) {

  private val delayTopicName = TopicMapper.getDelayTopicName(baseTopic)
  consumer.subscribe(util.Arrays.asList(delayTopicName))

  def poll(timeoutMs: Long): ConsumerRecords[Array[Byte], Array[Byte]] = {
    consumer.poll(timeoutMs)
  }

  import scala.collection.JavaConversions.asScalaSet

  def ensureAssignment(partitions: mutable.Set[TopicPartition], timeoutMs: Long) = {
    var assigned = false
    val beginTime = SystemTime.hiResClockMs
    val endTime = beginTime + timeoutMs
    while (!assigned && SystemTime.hiResClockMs < endTime) {
      consumer.poll(1000)
      val assignment = asScalaSet(consumer.assignment())
      if ((partitions -- assignment).size == 0) {
        assigned = true
      } else
        Thread.sleep(500)
    }
    if (!assigned)
      throw new IllegalStateException(s"can't assign $partitions")
  }

  def commit() = consumer.commitSync()
  def commitAsync() = consumer.commitAsync()

  def seek(offsets: Map[TopicPartition, OffsetAndMetadata]) = {
    ensureAssignment(asScalaSet(offsets.keySet()), 30000)
    val iter = offsets.entrySet().iterator()
    while (iter.hasNext) {
      val entry = iter.next()
      consumer.seek(entry.getKey, entry.getValue.offset())
    }
    consumer.commitSync(offsets)
  }

  def close(): Unit = {
    consumer.close()
  }
}

object MetaConsumer {
  val logger = LoggerFactory.getLogger(classOf[MetaConsumer])
}
