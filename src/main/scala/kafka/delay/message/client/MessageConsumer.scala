package kafka.delay.message.client

import java.util
import java.util.Properties

import kafka.delay.message.actor.TopicDelayService
import kafka.delay.message.actor.request.BatchConsumeResult
import kafka.delay.message.client.cache.{RecordCache, TempRecordCache}
import kafka.delay.message.exception.ImpossibleBatchException
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import kafka.delay.message.utils._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * TODO: 尽量减少它在底层所使用的consumer开销，比如减少序列化、选择合适的fetch size
  */
class MessageConsumer(baseTopic: String,
                      partition: Int,
                      bootstrapServers: String,
                      clientCreator: ClientCreator,
                      recordCache: Option[RecordCache]) extends MetaBasedConsumer {

  import MessageConsumer._

  private[this] val delayTopic = TopicMapper.getDelayTopicName(baseTopic)

  private[this] val consumer = getConsumer(baseTopic, partition, bootstrapServers, clientCreator)
  consumer.assign(util.Arrays.asList(new TopicPartition(delayTopic, partition)))

  private val topicPartition = new TopicPartition(delayTopic, partition)
  private val topicPartitionCollection = util.Arrays.asList(topicPartition)
  private val maxEmptyCount = 30
  private var currentEmptyCount = 0
  private val cacheOpt: Option[RecordCache] = recordCache
  var needMoreCacheSpace: Boolean = false

  // 处理各种异常情况。
  override def consume(metaBatch: OffsetBatch): BatchConsumeResult = {
    if (metaBatch.isEmpty) {
      BatchConsumeResult(Seq.empty, None)
    } else {
      val now = System.currentTimeMillis()
      val fetched = new mutable.ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]](metaBatch.size)
      val nonCachedOpt = getCached(metaBatch, fetched)
      logger.debug("{} cached size {}, non-cached size {}",
        topicPartition,
        fetched.size.toString,
        nonCachedOpt.fold(0)(_.size).toString)
//      logger.debug("partition: {} non cached size {}",
//        partition: Any,
//        nonCachedOpt.getOrElse(List.empty).size: Any)
      if (nonCachedOpt.isEmpty) {
        BatchConsumeResult(fetched, None)
      } else {
        val nonCached = nonCachedOpt.get
        val maxOffset = nonCached.last
        consumer.seek(topicPartition, nonCached.head)
        var complete = false
        while (!complete) {
          val records = consumer.poll(pollTimeout)
          checkEmptiness(nonCached, records)
          val iterator = records.iterator()
          while (iterator.hasNext) {
            val record = iterator.next()
            val offset = record.offset()
            if (!complete && nonCached.contains(offset)) {
              fetched += record
            } else {
              cacheOpt.foreach { cache =>
                if ((cache += (record, now))) {
                  needMoreCacheSpace = true
                }
              }
            }
            if (offset >= maxOffset) {
              complete = true
            }
          }
        }
        currentEmptyCount = 0
        countMissed(metaBatch, fetched.toArray)
      }
    }
  }

  /**
    * 把已经cache的消息加到buffer里，返回没有cache的消息的OffsetBatch
    */
  private def getCached(offsets: OffsetBatch,
                        buffer: mutable.ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]])
  : Option[OffsetBatch] = {
    if (cacheOpt.isEmpty)
      Some(offsets)
    else {
      val result = cacheOpt.get.get(offsets)
//      result.cached.foreach { records => buffer ++= records }
      buffer ++= result.cached
      result.nonCached
    }
  }

  /**
    * 有可能正常情况下poll也会拉取不到消息，所以需要多少尝试，再决定调用checkOffsetRange，因为它的开销比较大
    *
    * @param metaBatch
    * @param records
    */
  private def checkEmptiness(metaBatch: OffsetBatch, records: ConsumerRecords[Array[Byte], Array[Byte]]): Unit = {
    if (records.isEmpty) {
      currentEmptyCount += 1
      if (currentEmptyCount > maxEmptyCount) {
        currentEmptyCount = 0
        checkOffsetRange(metaBatch)
      }
    }
  }

  private def countMissed(metaBatch: OffsetBatch,
                          consumeResult: Array[ConsumerRecord[Array[Byte], Array[Byte]]]) = {
    //    for (c <- cacheOpt; r <- consumeResult) {
    //      c -= r.offset()
    //    }
    if (metaBatch.length != consumeResult.length) {
      logger.warn(s"in-complete batch of offsets number {}, result number {}",
        metaBatch.length,
        consumeResult.length)
      if (consumeResult.isEmpty) {
        BatchConsumeResult(consumeResult, Some(metaBatch))
      } else {
        val got = consumeResult.map(_.offset()).toSet
        val toGet = metaBatch.toSet
        val missed = toGet -- got
        BatchConsumeResult(consumeResult, Some(missed))
      }
    } else
      BatchConsumeResult(consumeResult, None)
  }

  /**
    * 注意，对此处抛出的异常需要单独处理，因为可能这批消息里有些是可以消费的。
    * TODO: 并行化。同时考虑到cache
    * @param metaBatches
    * @return
    */
  override def consume(metaBatches: OffsetBatchList): BatchConsumeResult = {
    val batchConsumerResults = metaBatches.batches.map(consume)
    val allMissed = batchConsumerResults.flatMap(_.missed).flatten
    BatchConsumeResult(batchConsumerResults.flatMap(_.fetched), if (allMissed.isEmpty) None else Some(allMissed))
  }


  /**
    * 定义:
    * LSO: Log Start Offset, 通过consumer的beginOffsets方法获得，这是当前TopicPartition的最小的可用的offset，除非此分区数据
    * 为空，此时，LSO == LEO
    * LEO: Log End Offset, 通过consumer的endOffsets方法获得，这是下一个将要被写入的消息将要被赋予的offset
    *
    * 那么，LEO，LSO和batch的min, max有以下几种有意义的关系：
    *
    * 1. LEO == LSO 此时分区数据为空，这个batch不可能被完成。应退出对这个batch的处理
    * 2. min >= LSO && max >= LEO, 那么有一部分batch内的消息超过了LEO，表示可能有消息丢失，应丢弃这个batch，否则可能带来错误
    * 3. min < LSO && max < LEO，小于LSO的那部分batch内的消息可能已经被删除，此时应该打印日志说明，并且继续处理这个batch.
    *
    * 4. max < LSO，所有消息已被删除。打印日志，然后退出对这个batch的处理
    * 5. min >= LEO, min超过了LEO，表示有消息丢失，应丢弃这个batch。
    * 6. min >= LSO && max < LEO, 可以获取整个batch，继续处理
    *
    */
  private def checkOffsetRange(batch: OffsetBatch): Unit = {
    logger.debug("checking offset range for {}", topicPartition)
    val LSO = consumer.beginningOffsets(topicPartitionCollection).get(topicPartition)
    val LEO = consumer.endOffsets(topicPartitionCollection).get(topicPartition)
    val min = batch.min
    val max = batch.max
    assert(LEO != null && LSO != null) // scalastyle:ignore
    if (LSO == LEO) {
      throw new ImpossibleBatchException(s"$topicPartition is empty for $batch", lso = LSO, leo = LEO)
    } else if (min >= LSO && max >= LEO) {
      throw new ImpossibleBatchException(s"message maybe lost for $batch",
        lso = LSO, leo = LEO)
    } else if (min < LSO && max < LEO) {
      logger.error("", new ImpossibleBatchException(s"some message can't fetched for $batch", lso = LSO, leo = LEO))
    } else if (max < LSO) {
      throw new ImpossibleBatchException(s"all messages have been deleted for $batch", lso = LSO, leo = LEO)
    } else if (min >= LEO) {
      throw new ImpossibleBatchException(s"message maybe lost for $batch", lso = LSO, leo = LEO)
    } else if (min >= LSO && max < LEO) {
      () //do nothing
    } else
      throw new ImpossibleBatchException(s"for $batch", lso = LSO, leo = LEO)
  }

  override def close(): Unit = {
    consumer.wakeup()
    consumer.close()
  }

  override def resetCacheSize(size: Int, version: Long): Unit = {
    needMoreCacheSpace = false
    cacheOpt.foreach(_.resize(size, System.currentTimeMillis(), version))
  }

  override def getCacheMetrics() = {
    val now = SystemTime.milliseconds
    cacheOpt.foreach(_.removeExpiredBefore(now - TopicDelayService.RecordCacheClearDuration))
    cacheOpt.map { c => c.metrics() }
  }

  /**
    * 将不在要消费batch里的消息暂存到cached里
    *
    * @param metaBatch 应该是sorted
    * @param cached
    * @return
    */
  override def consume(metaBatch: OffsetBatch,
                       cached: TempRecordCache) = {
    if (metaBatch.isEmpty) {
      BatchConsumeResult(Seq.empty, None)
    } else {
      val fetched = new mutable.ArrayBuffer[ConsumerRecord[Array[Byte], Array[Byte]]](metaBatch.size)
      val minOffset = metaBatch.head
      val maxOffset = metaBatch.last
      consumer.seek(topicPartition, minOffset)
      var complete = false
      while (!complete) {
        val records = consumer.poll(pollTimeout)
        checkEmptiness(metaBatch, records)
        val iterator = records.iterator()
        while (iterator.hasNext) {
          val record = iterator.next()
          val offset = record.offset()
          if (!complete && metaBatch.contains(offset)) {
            fetched += record
          } else {
            cached += record
          }
          if (offset >= maxOffset) {
            complete = true
          }
        }
      }
      currentEmptyCount = 0
      countMissed(metaBatch, fetched.toArray)
    }
  }

  /**
    * 加一些记录到cache
    *
    * @param records
    */
  override def addToCache(records: List[ConsumerRecord[Array[Byte], Array[Byte]]]): Unit = {
    val now = SystemTime.milliseconds
    cacheOpt.foreach { cache =>
      records.foreach { record =>
        if ((cache += (record, now))) {
          needMoreCacheSpace = true
        }
      }
    }
//    logger.debug("topic {}, partition {}, size after add {}, add to cache {}",
//      baseTopic.toString,
//      partition.toString,
//      cacheOpt.map(_.recordCount).getOrElse(0).toString,
//      records.map(_.offset()).size.toString
//    )
  }
}

object MessageConsumer {
  val pollTimeout = 1000L
  val logger = LoggerFactory.getLogger(MessageConsumer.getClass)

  def getConsumer(baseTopic: String,
                  partition: Int,
                  bootstrapServers: String,
                  clientCreator: ClientCreator): Consumer[Array[Byte], Array[Byte]] = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.messageConsumerGroup(baseTopic))
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none") //reset可以导致poll消息的offset不是连续的
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    clientCreator.getConsumer[Array[Byte], Array[Byte]](config)
  }
}
