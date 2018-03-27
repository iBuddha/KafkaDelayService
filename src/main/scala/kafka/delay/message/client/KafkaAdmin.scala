package kafka.delay.message.client

import java.util
import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.apache.kafka.common.{PartitionInfo, TopicPartition}

import scala.collection.mutable

object KafkaAdmin {
  def latestCommittedOffset(bootstrapServers: String, topic: String, partitionNumber: Int, groupId: String): Map[Int, Option[OffsetAndMetadata]] = {
    var consumer: Consumer[Array[Byte], Array[Byte]] = null
    try {
      consumer = getConsumer(bootstrapServers, groupId)
      consumer.subscribe(util.Arrays.asList(topic))
      (0 until partitionNumber).map { partitionId =>
        partitionId -> Option(consumer.committed(new TopicPartition(topic, partitionId)))
      }.toMap

    } finally {
      if (consumer != null)
        consumer.close()
    }
  }

  def topicInfo(bootstrapServers: String, topicName: String): Option[Seq[PartitionInfo]] = {
    var consumer: Consumer[Array[Byte], Array[Byte]] = null
    try {
      consumer = getConsumer(bootstrapServers, "delay-server-admin")
      val topics = consumer.listTopics()
      val topicInfo = topics.get(topicName)
      if (topicInfo == null)
        None
      else {
        Some(scala.collection.JavaConverters.asScalaBufferConverter(topicInfo).asScala)
      }
    } finally {
      if (consumer != null)
        consumer.close()
    }
  }

  def topics(bootstrapServers: String) = {
    import scala.collection.JavaConverters._
    var consumer: Consumer[Array[Byte], Array[Byte]] = null
    try {
      consumer = getConsumer(bootstrapServers, "delay-server-admin")
      val infoIterator = consumer.listTopics().entrySet().iterator()
      val topics = mutable.Map.empty[String, List[PartitionInfo]]
      while(infoIterator.hasNext) {
        val info = infoIterator.next()
        topics += (info.getKey -> info.getValue.asScala.toList)
      }
      topics
    } finally {
      if (consumer != null)
        consumer.close()
    }
  }

  def topicInfos(bootstrapServers: String, topics: Seq[String]): Map[String, Seq[PartitionInfo]] = {
    var consumer: Consumer[Array[Byte], Array[Byte]] = null
    try {
      consumer = getConsumer(bootstrapServers, "delay-server-admin")
      val topicInfos = consumer.listTopics()
      var result = Map.empty[String, Seq[PartitionInfo]]
      topics.foreach { topicName =>
        val topicInfo = topicInfos.get(topicName)
        if (topicInfo != null)
          result = result + (topicName -> scala.collection.JavaConverters.asScalaBufferConverter(topicInfo).asScala)
      }
      result
    } finally {
      if (consumer != null)
        consumer.close()
    }
  }

  def beginOffsets(bootstrapServers: String, topicName: String) =
    beginOrEndOffset(bootstrapServers, topicName, true)

  def endOffsets(bootstrapServers: String, topicName: String) =
    beginOrEndOffset(bootstrapServers, topicName, false)


  private def beginOrEndOffset(bootstrapServers: String,
                               topicName: String,
                               begin: Boolean): Map[TopicPartition, Long] = {
    import scala.collection.JavaConversions._
    var consumer: Consumer[Array[Byte], Array[Byte]] = null
    try {
      consumer = getConsumer(bootstrapServers, "delay-server-admin")
      val topicInfos = consumer.listTopics()
      val topicInfo = topicInfos.get(topicName)
      val partitions = topicInfo.map(pInfo => new TopicPartition(topicName, pInfo.partition()))
      val offsets = if (begin)
        consumer.beginningOffsets(partitions)
      else
        consumer.endOffsets(partitions)
      offsets.map {
        case (partitionInfo, offset) => (partitionInfo, offset.toLong)
      }.toMap
    } finally {
      if (consumer != null)
        consumer.close()
    }
  }

  private def getConsumer(bootstrapServers: String, groupId: String): Consumer[Array[Byte], Array[Byte]] = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId) // special consumer group
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    new KafkaConsumer[Array[Byte], Array[Byte]](config)
  }
}
