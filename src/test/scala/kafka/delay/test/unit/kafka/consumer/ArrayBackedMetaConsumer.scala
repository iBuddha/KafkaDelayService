package kafka.delay.test.unit.kafka.consumer

import java.io.IOException
import java.util.regex.Pattern
import java.{lang, util}

import kafka.delay.message.storage.AbstractKafkaMetaStorage.{MetaKey, MetaValue}
import kafka.utils.nonthreadsafe
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.{Metric, MetricName, PartitionInfo, TopicPartition}

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
  * 它从内部的一个缓存拉取消息，可以通过add添加消息
  *
  * @param topic
  * @param maxPollRecords 每个partition每次poll最多返回这么多records
  */
@nonthreadsafe
class ArrayBackedMetaConsumer(topic: String, partitionNumber: Int, autoReset: OffsetResetStrategy, maxPollRecords: Int) extends Consumer[MetaKey, MetaValue] {
  private val messages: mutable.Map[Int, mutable.ArrayBuffer[ConsumerRecord[MetaKey, MetaValue]]] = mutable.Map.empty
  private val committedOffset: mutable.Map[Int, Long] = mutable.Map.empty
  private val currentConsumePosition: mutable.Map[Int, Long] = mutable.Map.empty
  private val subscriptionState: util.Set[TopicPartition] = new util.HashSet()

  def add(topic: String, partition: Int, key: MetaKey, value: java.lang.Long): Int = {
    if (topic != this.topic)
      throw new IllegalArgumentException(s"only allowed to add message for topic ${this.topic}")
    val parMessages = messages.getOrElseUpdate(partition, mutable.ArrayBuffer.empty)
    parMessages += new ConsumerRecord(topic, partition, parMessages.size, key, value)
    parMessages.size - 1
  }

  def add(partition: Int, key: MetaKey, value: java.lang.Long): Int = {
    add(topic, partition, key, value)
  }

  def addRandom(partition: Int, count: Int): Unit = {
    (0 until count).foreach { _ =>
      add( partition, randomKey(), long2Long(Random.nextLong()))
    }
  }

  override def paused(): util.Set[TopicPartition] = throw new UnsupportedOperationException

  override def commitSync(): Unit = {
    currentConsumePosition foreach {
      case (partition, offset) => committedOffset.put(partition, offset)
    }
  }

  override def commitSync(offsets: util.Map[TopicPartition, OffsetAndMetadata]): Unit = {
    committedOffset ++= offsets.map {
      case (tp, offsetAndMeta) => (tp.partition(), offsetAndMeta.offset())
    }
  }

  override def poll(timeout: Long): ConsumerRecords[MetaKey, MetaValue] = {
    val records: java.util.HashMap[TopicPartition, java.util.List[ConsumerRecord[MetaKey, MetaValue]]] = new util.HashMap()
    subscriptionState.foreach { tp =>
      val currentOffset = currentConsumePosition.getOrElseUpdate(tp.partition(),
        if (autoReset == OffsetResetStrategy.LATEST)
          messages.getOrElseUpdate(tp.partition(), ArrayBuffer.empty).size
        else
          0L
      )
      val partitionRecords = getRecords(tp.partition(), currentOffset.toInt, maxPollRecords)
      val polled = partitionRecords.size
      currentConsumePosition.update(tp.partition(), currentOffset + polled)
      records.put(tp, partitionRecords)
    }
    new ConsumerRecords[MetaKey, MetaValue](records)
  }

  /**
    *
    * @param partitionId
    * @param from   included
    * @param count max number, if there aren't so many remaining, return all available ones
    * @return
    */
  private def getRecords(partitionId: Int, from: Int, count: Int): Seq[ConsumerRecord[MetaKey, MetaValue]] = {
    if (!messages.contains(partitionId))
      Seq.empty
    else {
      messages.get(partitionId).get.slice(from, from + count)
    }
  }


  override def subscription(): util.Set[String] = subscriptionState.map(_.topic())

  override def seek(partition: TopicPartition, offset: Long): Unit =
    currentConsumePosition.update(partition.partition(), offset)

  override def listTopics(): util.Map[String, util.List[PartitionInfo]] =
    throw new UnsupportedOperationException

  override def unsubscribe(): Unit = subscriptionState.clear()

  override def seekToEnd(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.foreach { p =>
      if (!subscriptionState.contains(p))
        throw new IOException(s"consumer group isn't assigned $p")
      val leo = messages.get(p.partition()).map(_.size).getOrElse(0)
      currentConsumePosition.put(p.partition(), leo)
    }
  }

  override def close(): Unit = {}

  override def offsetsForTimes(timestampsToSearch: util.Map[TopicPartition, lang.Long]): util.Map[TopicPartition, OffsetAndTimestamp] =
    throw new UnsupportedOperationException

  override def resume(partitions: util.Collection[TopicPartition]): Unit =
    throw new UnsupportedOperationException


  override def endOffsets(partitions: util.Collection[TopicPartition]): util.Map[TopicPartition, lang.Long] = {
    val result = partitions.map { p =>
      if (!subscriptionState.contains(p))
        throw new IOException(s"consumer group isn't assigned $p")
      val leo = messages.get(p.partition()).map(_.size).getOrElse(0).toLong
      p -> long2Long(leo)
    }.toMap
    mapAsJavaMap(result)
  }

  override def committed(partition: TopicPartition): OffsetAndMetadata = {
    if (!subscriptionState.contains(partition)) {
      throw new IOException(s"didn't subscribed $partition")
    }
    committedOffset.get(partition.partition()).map(new OffsetAndMetadata(_)).getOrElse(null)
  }

  override def subscribe(topics: util.Collection[String]): Unit = {
    unsubscribe()
    if (topics.size() > 2 || topics.head != topic)
      throw new IllegalArgumentException(s"only support subscription to $topic")
    (0 until partitionNumber).foreach(p => subscriptionState.add(new TopicPartition(topic, p)))
  }

  override def subscribe(topics: util.Collection[String], callback: ConsumerRebalanceListener): Unit =
    throw new UnsupportedOperationException

  override def subscribe(pattern: Pattern, callback: ConsumerRebalanceListener): Unit =
    throw new UnsupportedOperationException

  override def wakeup(): Unit = {}

  override def assignment(): util.Set[TopicPartition] = subscriptionState

  override def beginningOffsets(partitions: util.Collection[TopicPartition]): util.Map[TopicPartition, lang.Long] =
    mapAsJavaMap(partitions.map(p => (p, long2Long(0L))).toMap)

  override def pause(partitions: util.Collection[TopicPartition]): Unit =
    throw new UnsupportedOperationException

  override def commitAsync(): Unit = throw new UnsupportedOperationException

  override def commitAsync(callback: OffsetCommitCallback): Unit = throw new UnsupportedOperationException

  override def commitAsync(offsets: util.Map[TopicPartition, OffsetAndMetadata], callback: OffsetCommitCallback): Unit =
    throw new UnsupportedOperationException

  override def partitionsFor(topic: String): util.List[PartitionInfo] = throw new UnsupportedOperationException

  override def position(partition: TopicPartition): Long = {
    if (!subscriptionState.contains(partition))
      throw new IOException(s"didn't subscribe to $partition")
    currentConsumePosition.getOrElse(partition.partition(), 0L)
  }

  override def metrics(): util.Map[MetricName, _ <: Metric] =
    throw new UnsupportedOperationException

  override def seekToBeginning(partitions: util.Collection[TopicPartition]): Unit = {
    partitions.foreach(p => {
      if (p.topic() != topic)
        throw new IllegalArgumentException(s"didn't subscribed to ${p.topic}")
      if (!subscriptionState.contains(p))
        throw new IOException(s"didn't assigned $p")
      currentConsumePosition.put(p.partition(), 0L)
    })
  }

  override def assign(partitions: util.Collection[TopicPartition]): Unit = {
    if (partitions.find(_.topic() != topic).isDefined)
      throw new IllegalArgumentException(s"can only assign partitions of $topic")
    unsubscribe()
    subscriptionState.addAll(partitions)
  }

  def randomKey() = {
    MetaKey(partition = Random.nextInt(partitionNumber), offset = Random.nextInt())
  }
}

