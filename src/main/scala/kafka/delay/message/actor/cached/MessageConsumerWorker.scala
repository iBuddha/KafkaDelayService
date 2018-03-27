package kafka.delay.message.actor.cached

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import kafka.delay.message.actor.cached.CacheRequests.{AddCache, PreConsume}
import kafka.delay.message.actor.request.CacheAdd
import kafka.delay.message.client.cache.RecordSizeSampler
import kafka.delay.message.client.parser.RecordExpireTimeParser
import kafka.delay.message.timer.meta.{ArrayOffsetBatch, OffsetBatch}
import kafka.delay.message.utils._
import org.apache.kafka.clients.consumer._
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.util.{Random, Try}

class MessageConsumerWorker(cachedConsumerActor: ActorRef,
                            bootstrapServers: String,
                            baseTopic: String,
                            partition: Int,
                            expirationParser: RecordExpireTimeParser) extends Actor with ActorLogging {

  import MessageConsumerWorker._

  private var consumerOpt: Option[KafkaConsumer[Array[Byte], Array[Byte]]] = None
  private val properties = getProperties
  private val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
  private val tp = new TopicPartition(delayTopic, partition)
  private val scheduler = context.system.scheduler
  private var scheduledTask: Option[Cancellable] = None
  private var lastWorkTime = 0L
  private var recordSize = 0
  private var latestKnownLSO = -1L
  private var latestKnownLEO = -1L


  override def preStart(): Unit = {
    buildConsumer
    scheduledTask = Some(scheduler.schedule(IdleCheckInterval, IdleCheckInterval, self, IdleCheck)(context.system.dispatcher))
    super.preStart()
  }

  override def postStop(): Unit = {
    Try{closeConsumer}
    scheduledTask.foreach(_.cancel())
    super.postStop()
  }

  override def receive = {
    case r@PreConsume(id, batch, maxMsFromMini, requestTime, retryCount, addLocal) => {
      val lag = System.currentTimeMillis() - requestTime
      if (lag > RequestExpireMs || retryCount >= 5) {
        log.warning("PreConsume {} lag {}ms retry count {}, abandon it", id, lag, retryCount)
      } else if (lag > RetryCacheExpireMs && retryCount > 0 && retryCount < 5) {
        log.debug("PreConsume {} lag {}ms too much, retry it with retryCount {}", id, lag, retryCount)
        sender ! r
      } else {
        //        log.debug("warmer will fetch {} records", batch.size)
        if (batch.isEmpty) {
          cachedConsumerActor ! CacheAdd(List.empty)
        } else {
          if (consumerOpt.isEmpty) {
            buildConsumer
          }
          try {
            if (log.isDebugEnabled) {
              val start = System.currentTimeMillis()
              val consumedAndMissed = consume(lag, id, ConsumeTimeout, maxMsFromMini, batch, addLocal)
              val end = System.currentTimeMillis()
              val batchRange = batch.last - batch.head
              log.debug(s"PreConsume $id cost {} for batch range {} fetched {} missed {}",
                end - start,
                batchRange,
                consumedAndMissed.batchConsumed.size + consumedAndMissed.extraConsumed.size,
                consumedAndMissed.missed.size)
              maybeRetryMissed(r, consumedAndMissed)
              cachedConsumerActor ! AddCache(
                consumedAndMissed.batchConsumed,
                consumedAndMissed.extraConsumed,
                requestTime,
                addLocal)
            } else {
              val consumedAndMissed = consume(lag, id, ConsumeTimeout, maxMsFromMini, batch, addLocal)
              cachedConsumerActor ! AddCache(
                consumedAndMissed.batchConsumed,
                consumedAndMissed.extraConsumed,
                requestTime,
                addLocal)
              maybeRetryMissed(r, consumedAndMissed)
            }
          } catch {
            case _: OffsetOutOfRangeException =>
              handleOffsetOutOfRange(batch)
          }
        }
      }
    }

    case IdleCheck => {
      if (System.currentTimeMillis() - lastWorkTime > MaxIdleMs) {
        closeConsumer
      }
    }
  }

  private def handleOffsetOutOfRange(batch: OffsetBatch): Unit = {
    latestKnownLSO = getLso()
    latestKnownLEO = getLeo()
    log.debug("out of range batch min {} max {}, lso {}, leo",
      batch.head,
      batch.last,
      latestKnownLSO,
      latestKnownLEO
    )
  }

  private def maybeRetryMissed(req: PreConsume, result: ConsumedAndMissed) = {
    if (!result.missed.isEmpty) {
      val offsets = mutable.Set.empty ++= req.batch
      val minOffset = offsets.min
      val maxOffset = offsets.max
      val head = offsets.head
      log.info(s"PreConsume {} minOffset $minOffset maxOffset $maxOffset head $head has {} messages not consumed, retry it {}",
        req.id,
        result.missed.size,
        if (result.missed.size < 100) result.missed else "")
      sender() ! req.copy(batch = new ArrayOffsetBatch(result.missed.toArray.sorted))
    }
  }

  //TODO: schedule empty poll, to avoid leave group

  private def consume(delayMs: Long,
                      requestId: Long,
                      timeout: Int,
                      maxMsFromMini: Long,
                      batch: OffsetBatch,
                      direct: Boolean): ConsumedAndMissed = {
    var offsets = mutable.Set.empty ++= batch
    val minOffset = offsets.min
    val maxOffset = offsets.max
    if (minOffset < latestKnownLSO || maxOffset < latestKnownLSO) {
      ConsumedAndMissed(List.empty, List.empty, Set.empty)
    } else {
      val startTime = SystemTime.hiResClockMs
      val consumer = consumerOpt.get
      consumer.seek(tp, minOffset)
      val batchConsumedBuffer = mutable.ListBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      val extraConsumedBuffer = mutable.ListBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      var complete = false
      val pollInterval = 128
      val maxEmptyPoll = 2
      var currentEmptyPoll = 0
      val InitMinMs = -1L
      var minExpirationMs = InitMinMs
      var maxExpireMs = InitMinMs + maxMsFromMini
      while (!complete) {
        val pollRecordNumber =
          if (batchConsumedBuffer.isEmpty) {
            Math.min(DefaultMaxPollRecords, maxOffset - minOffset + 1).toInt
          } else {
            Math.min(maxOffset - batchConsumedBuffer.last.offset() + 1, DefaultMaxPollRecords).toInt
          }
        val pollRecordSize = Math.min(pollRecordNumber * recordSize, DefaultMaxPartitionFetchBytes)
        val prefetchEnable = maxEmptyPoll > PrefetchThreshold
        val records =
          if (!direct && delayMs < LowPressureDelayThreshold) {
            consumer.poll(prefetchEnable, DefaultMaxPollRecords, pollRecordSize, pollInterval)
          } else {
            consumer.poll(prefetchEnable, pollRecordNumber, pollRecordSize, pollInterval)
          }
        //      val records = consumer.poll(pollInterval)
        if (records.isEmpty) {
          currentEmptyPoll += 1
          if (offsets.isEmpty) {
            complete = true
          } else {
            val usedTime = SystemTime.hiResClockMs - startTime
            if (currentEmptyPoll > maxEmptyPoll || usedTime > timeout) {
              complete = true
              log.info(s"PreConsume $requestId used time {} with {} empty polls, no more polls, {} to fetch, batchSize {}",
                usedTime,
                currentEmptyPoll,
                offsets.size,
                batch.size
              )
            }
          }
        } else {
          val iterator = records.iterator()
          while (iterator.hasNext) {
            val record = iterator.next()
            val recordOffset = record.offset()
            if (recordOffset < minOffset) {
              if (log.isDebugEnabled) {
                log.debug("got stale records from last poll")
              }
            }
            if (recordOffset >= minOffset && offsets.contains(recordOffset)) {
              batchConsumedBuffer += record
              offsets -= recordOffset
              if (minExpirationMs == InitMinMs) {
                minExpirationMs = expirationParser.getExpireMs(record)
                maxExpireMs = minExpirationMs + maxMsFromMini
              }
            } else if (extraConsumedBuffer.size < MaxRecordNumber) {
              val expireMs = expirationParser.getExpireMs(record)
              if (!direct && minExpirationMs != InitMinMs &&
                expireMs > minExpirationMs &&
                expireMs < maxExpireMs) {
                extraConsumedBuffer += record
              }
            }
            if (record.offset() >= maxOffset || offsets.isEmpty) {
              //            if (!offsets.isEmpty) {
              //              //              log.debug("{} exceeded maxOffset, but still has {} to consume", record.offset(), maxOffset, offsets.size)
              //            }
              complete = true
            }
          }
        }
        val timeCost = SystemTime.hiResClockMs - startTime
        if (timeCost > timeout) {
          //        log.debug("PreConsume {} timeout after {}, {} to consume", requestId, timeCost, offsets.size)
          complete = true
        }
      }
      lastWorkTime = System.currentTimeMillis()
      if (!batchConsumedBuffer.isEmpty) {
        recordSize = RecordSizeSampler.bytes(batchConsumedBuffer.head)
      }
      ConsumedAndMissed(batchConsumedBuffer.toList, extraConsumedBuffer.toList, offsets.toSet)
    }
  }

  private def buildConsumer = {
    lastWorkTime = System.currentTimeMillis()
    val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](properties)
    consumerOpt = Some(kafkaConsumer)
    kafkaConsumer.assign(util.Arrays.asList(tp))
    latestKnownLSO = getLso()
    latestKnownLEO = getLeo()
  }

  private def getLso() = consumerOpt.get.beginningOffsets(util.Arrays.asList(tp)).get(tp)
  private def getLeo() = consumerOpt.get.endOffsets(util.Arrays.asList(tp)).get(tp)

  private def closeConsumer = {
    consumerOpt.foreach { c =>
      c.close()
      log.debug("consumer closed")
    }
    consumerOpt = None
  }


  private def getProperties: Properties = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.cacheWarmerConsumerGroup(baseTopic))
    //如果没有设置auto reset policy，当out of range 时，就会抛 org.apache.kafka.clients.consumer.OffsetOutOfRangeException: Offsets out of range with no configured reset policy for partitions: {foo-delay-0=14574152}
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "none") //reset可以导致poll消息的offset不是连续的
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, DefaultMaxPollRecords.toString)
    config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, DefaultMaxPartitionFetchBytes.toString)
    config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 30000.toString)
    config.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30001.toString)
    config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 30000.toString)
    config
  }
}

object MessageConsumerWorker {
  import Time._
  val RequestExpireMs = 5 * MsPerSec
  val RetryCacheExpireMs = 1024 * MsPerSec
  val MaxIdleMs = 30 * MsPerSec * SecsPerMin + Math.abs(Random.nextInt(1000)) //with linger time
  val ConsumeTimeout = 1 * MsPerSec
  val IdleCheckInterval = FiniteDuration(5 * MsPerSec * SecsPerMin, TimeUnit.MILLISECONDS)
  val DefaultMaxPollRecords = 1000
  val DefaultMaxPartitionFetchBytes = 16 * Size.nKB
  val LowPressureDelayThreshold = 10
  val PrefetchThreshold = 100
  val MaxRecordNumber = 200 //需要控制每次添加到cache的消息的数量，否则突然添加大量的消息，可能会使cache在请求新的空间之前被迫移除已有元素

  case class ConsumedAndMissed(batchConsumed: List[ConsumerRecord[Array[Byte], Array[Byte]]],
                               extraConsumed: List[ConsumerRecord[Array[Byte], Array[Byte]]],
                               missed: Set[Long])

  object IdleCheck

  def sorted(batch: OffsetBatch): Boolean = {
    var pre = batch.head
    batch.foldLeft(true) { (sorted, current) =>
      if (!sorted)
        false
      else {
        if (current < pre)
          false
        else {
          pre = current
          true
        }
      }
    }
  }
}