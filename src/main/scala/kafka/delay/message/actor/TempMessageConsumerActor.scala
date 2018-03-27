package kafka.delay.message.actor

import java.util
import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.actor.{Actor, ActorLogging, Cancellable}
import kafka.delay.message.actor.request.{BatchConsumeResult, CacheAdd, Warm}
import kafka.delay.message.timer.meta.OffsetBatch
import kafka.delay.message.utils.{GroupNames, SystemTime, Time, TopicMapper}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration

class TempMessageConsumerActor(bootstrapServers: String,
                               baseTopic: String,
                               partition: Int) extends Actor with ActorLogging {
  import TempMessageConsumerActor._

  private var consumerOpt: Option[Consumer[Array[Byte], Array[Byte]]] = None
  private val properties = getProperties
  private val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
  private val tp = new TopicPartition(delayTopic, partition)
  private val scheduler = context.system.scheduler
  private var scheduledTask: Option[Cancellable] = None
  private var lastWorkTime = 0L


  override def preStart(): Unit = {
//    buildConsumer
    scheduledTask = Some(scheduler.schedule(IdleCheckInterval, IdleCheckInterval, self, IdleCheck)(context.system.dispatcher))
    super.preStart()
  }

  override def postStop(): Unit = {
    closeConsumer
    scheduledTask.foreach(_.cancel())
    super.postStop()
  }

  override def receive = {
    case Warm(messageConsumerActor, batch, requestTime) => {
      val lag = System.currentTimeMillis() - requestTime
      if (lag > RequestExpireMs) {
        log.warning("lag {}ms too much, abandon it", lag)
      } else {
//        log.debug("warmer will fetch {} records", batch.size)
        if (batch.isEmpty) {
          messageConsumerActor ! CacheAdd(List.empty)
        } else {
          if (consumerOpt.isEmpty) {
            buildConsumer
          }
          val consumed = consume(10000, batch)
          messageConsumerActor ! CacheAdd(consumed)
        }
        lastWorkTime = System.currentTimeMillis()
      }
    }
    case IdleCheck => {
      if(System.currentTimeMillis() - lastWorkTime > MaxIdleMs) {
        closeConsumer
      }
    }
  }

  private def consume(timeout: Int, batch: OffsetBatch) = {
    var offsets = batch.toSet
    val startTime = SystemTime.hiResClockMs
    val consumer = consumerOpt.get
    val buffer = mutable.ListBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
    consumer.seek(tp, batch.head)
    val max = batch.last
    var complete = false
    val pollInterval = 200
    val maxEmptyPoll = 5000 / pollInterval
    var currentEmptyPoll = 0
    while (!complete) {
      val records = consumer.poll(pollInterval)
      if (records.isEmpty) {
        currentEmptyPoll += 1
        if (currentEmptyPoll > maxEmptyPoll) {
          complete = true
        }
      } else {
        val iterator = records.iterator()
        while (iterator.hasNext && !complete) {
          val record = iterator.next()
          val recordOffset = record.offset()
          if (offsets.contains(recordOffset)) {
            buffer += record
            offsets -= recordOffset
          }
          if (record.offset() >= max || offsets.isEmpty) {
            complete = true
          }
        }
      }
      if (SystemTime.hiResClockMs - startTime > timeout) {
        complete = true
      }
    }
    buffer.toList
  }

  private def buildConsumer = {
    val kafkaConsumer = new KafkaConsumer[Array[Byte], Array[Byte]](properties)
    kafkaConsumer.assign(util.Arrays.asList(tp))
    kafkaConsumer.poll(0)
    consumerOpt = Some(kafkaConsumer)
  }

  private def closeConsumer = {
    consumerOpt.foreach(_.close())
    consumerOpt = None
  }


  private def getProperties: Properties = {
    val config = new Properties
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.cacheWarmerConsumerGroup(baseTopic))
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest") //reset可以导致poll消息的offset不是连续的
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    config
  }
}

object TempMessageConsumerActor {
  val RequestExpireMs = 1 * Time.SecsPerMin * Time.MsPerSec
  val MaxIdleMs = 5 * Time.SecsPerMin * Time.MsPerSec
  val IdleCheckInterval = FiniteDuration(MaxIdleMs, TimeUnit.MILLISECONDS)
  object IdleCheck
}