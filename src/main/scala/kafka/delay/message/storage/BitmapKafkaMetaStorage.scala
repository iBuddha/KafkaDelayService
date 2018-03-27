package kafka.delay.message.storage

import java.util
import java.util.Properties

import kafka.delay.message.storage.AbstractKafkaMetaStorage.{MetaKey, MetaValue}
import kafka.delay.message.timer.MessageTimer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.{GroupNames, Time, TopicMapper, Utils}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, LongDeserializer}
import org.roaringbitmap.RoaringBitmap
import org.slf4j.LoggerFactory

import scala.collection.JavaConversions._

class BitmapKafkaMetaStorage(bootstrapServers: String, baseTopic: String, time: Time)
  extends AbstractKafkaMetaStorage(bootstrapServers, TopicMapper.getMetaTopicName(baseTopic)) {

  import BitmapKafkaMetaStorage._

  /**
    * 恢复一个topic的所有timer到正确状态。使得TopicDelayService的MessageConsumer可以从被返回的位置继续处理。
    *
    * @param utilDelayTopicOffset  如果对于某个partition没有指定，就为Long.MAX. 注意是until, 结果不包括这个offset
    * @param timers
    * @param ignoredExpireBeforeMs 不处理在此之前的消息，这是一个clock time。注意是before,即包括这个ms
    * @return
    */
  override def restore(base: String,
                       utilDelayTopicOffset: Map[Int, Long],
                       timers: Seq[MessageTimer],
                       ignoredExpireBeforeMs: Long): Unit = {
    assert(base == baseTopic)
    val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
    var delayTopicConsumer: Consumer[Array[Byte], Array[Byte]] = null
    var delayPartitions: Seq[TopicPartition] = null
    var delayTopicBeginOffsets: java.util.Map[TopicPartition, java.lang.Long] = null
    try {
      delayTopicConsumer = getDelayTopicConsumer()
      delayPartitions = delayTopicConsumer.partitionsFor(delayTopic).map(p => new TopicPartition(p.topic, p.partition))
      delayTopicBeginOffsets = delayTopicConsumer.beginningOffsets(delayPartitions)
    } finally {
      if (delayTopicConsumer != null)
        delayTopicConsumer.close()
    }

    delayPartitions.foreach{p =>
      logger.info("find delay topic partition {}", p)
    }

    if(delayPartitions.toSet.size != timers.size){
      logger.error("")
      throw new IllegalStateException(s"delay topic has ${delayPartitions.size} partitions," +
        s" but has ${timers.size} timers")
    }

    var metaTopicConsumer: Consumer[MetaKey, java.lang.Long] = null
    try {
      metaTopicConsumer = getMetaTopicConsumer()
      delayPartitions.foreach(p => {
        restore(metaTopicConsumer,
          p.partition(),
          ignoreDelayTopicOffsetFrom = utilDelayTopicOffset.get(p.partition()).getOrElse(0L),
          timer = timers(p.partition()),
          ignoredExpireBeforeMs,
          delayTopicBeginOffsets.get(p)
        )
      })
      logger.info("restore delay service for {} state completed", baseTopic)
    } finally {
      if (metaTopicConsumer != null)
        metaTopicConsumer.close()
    }
  }


  /**
    * 前置条件：delay topic里消息的顺序和 meta topic里消息的顺序是一致的
    *
    * @param consumer
    * @param partitionId                要读取的meta topic的分区，它也对应于其它相关topic同样id的分区
    * @param ignoreDelayTopicOffsetFrom 不会处理在delay topic里对应的offset大于或者等于它的消息
    * @param timer
    * @param ignoredExpireBeforeMs      不会处理超时时间小于它的消息. 注意等于它的消息会被处理
    * @param delayTopicLSO              delay topic在这个分区的log start offset, 所有小于这个offset的消息会被忽略
    */
  def restore(consumer: Consumer[MetaKey, MetaValue],
              partitionId: Int,
              ignoreDelayTopicOffsetFrom: Long,
              timer: MessageTimer,
              ignoredExpireBeforeMs: Long,
              delayTopicLSO: Long): Unit = {
    logger.info("restoring for  {}#{}, ignore delay topic offset from {}," +
      " ignore messages expired before {}, delay topic log start offset: {}",
      baseTopic,
      partitionId.toString,
      ignoreDelayTopicOffsetFrom.toString,
      Utils.milliToString(ignoredExpireBeforeMs),
      delayTopicLSO.toString)
    consumer.unsubscribe()
    val metaTp = new TopicPartition(TopicMapper.getMetaTopicName(baseTopic), partitionId)
    consumer.assign(util.Arrays.asList(metaTp))
    val beginOffset = consumer.beginningOffsets(util.Arrays.asList(metaTp)).get(metaTp)
    val endOffset = consumer.endOffsets(util.Arrays.asList(metaTp)).get(metaTp)
    val bitmap = new RoaringBitmap()

    def addToBitmap(record: ConsumerRecord[MetaKey, MetaValue]) = {
      //总是先读到value不为null的消息，然后再标记为null
      val delayMessageOffset = record.key().offset
      val bitmapOffset = delayMessageOffset - delayTopicLSO
      if (bitmapOffset > Int.MaxValue)
        throw new Error(s"too many unprocessed delay messages, delayTopicLSO: $delayTopicLSO, delay message offset")
      if (record.value() != null && delayMessageOffset >= delayTopicLSO && record.value() >= ignoredExpireBeforeMs) {
        bitmap.add(bitmapOffset.toInt)
      } else {
        val bitmapOffsetInt = bitmapOffset.toInt
        if (record.value() == null && bitmap.contains(bitmapOffsetInt))
          bitmap.remove(bitmapOffsetInt)
      }
    }

    def addToTimer(record: ConsumerRecord[MetaKey, MetaValue]) = {
      val delayMessageOffset = record.key().offset
      val bitmapOffset = delayMessageOffset - delayTopicLSO
      if (bitmap.contains(bitmapOffset.toInt)) {
        try {
          timer.add(DelayMessageMeta.fromClockTime(delayMessageOffset, record.value(), time))
        } catch {
          case _: Exception =>
        }
      }
    }

    forEachQualified(consumer, partitionId, beginOffset, endOffset, ignoreDelayTopicOffsetFrom, timer, addToBitmap)
    forEachQualified(consumer, partitionId, beginOffset, endOffset, ignoreDelayTopicOffsetFrom, timer, addToTimer)
  }

  /**
    * 前置条件：delay topic里消息的顺序和 meta topic里消息的顺序是一致的
    *
    * 这个方法遍历在meta topic里范围为[beginOffset, endOffset)的消息，找出其中在delay topic里对应的offset小于
    * delayTopicMaxOffset的消息，然后对其应用指定的函数func
    *
    * @param consumer
    * @param partition           要读取的meta topic的分区，它也对应于其它相关topic同样id的分区
    * @param beginOffset         include
    * @param endOffset           exclude
    * @param delayTopicMaxOffset 不会处理在delay topic里对应的offset大于或者等于它的消息
    * @param timer
    */
  def forEachQualified(consumer: Consumer[MetaKey, MetaValue],
                       partition: Int,
                       beginOffset: Long,
                       endOffset: Long,
                       delayTopicMaxOffset: Long,
                       timer: MessageTimer,
                       func: ConsumerRecord[MetaKey, MetaValue] => Unit): Unit = {
    consumer.unsubscribe()
    val tp = new TopicPartition(TopicMapper.getMetaTopicName(baseTopic), partition)
    val targetTp = util.Arrays.asList(tp)
    consumer.assign(targetTp)
    consumer.poll(1000)
    var completed = false
    if (beginOffset < endOffset) {
      consumer.seekToBeginning(targetTp)
      while (!completed) {
        val records = consumer.poll(1000)
        val iter = records.iterator()
        while (iter.hasNext) {
          val record = iter.next()
          if (record.offset() >= endOffset - 1) {
            completed = true
          }
          if (record.offset() < endOffset && record.key().offset < delayTopicMaxOffset) {
            func(record)
          } else {
            //ignore
          }
        }
      }
    }
  }

  //expose for test
  protected def getMetaTopicConsumer(): Consumer[MetaKey, MetaValue] = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.storageConsumerGroup(baseTopic)) // special consumer group
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[MetaKeyDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getCanonicalName)
    new KafkaConsumer[MetaKey, java.lang.Long](config)
  }

  protected def getDelayTopicConsumer(): Consumer[Array[Byte], Array[Byte]] = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.metaConsumerGroup(baseTopic)) // special consumer group
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    new KafkaConsumer[Array[Byte], Array[Byte]](config)
  }
}

object BitmapKafkaMetaStorage {
  val logger = LoggerFactory.getLogger(BitmapKafkaMetaStorage.getClass)
}
