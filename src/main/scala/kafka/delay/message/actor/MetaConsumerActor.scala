package kafka.delay.message.actor

import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef}
import kafka.delay.message.actor.MetaConsumerActor.{Start, Started}
import kafka.delay.message.client.parser.ExpireTimeParser
import kafka.delay.message.client.{MetaConsumer, OffsetReset}
import kafka.delay.message.storage.{MetaStorage, StoreMeta}
import kafka.delay.message.timer.MessageTimer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.{GroupNames, SystemTime}
import org.apache.kafka.clients.consumer.{Consumer, ConsumerConfig, KafkaConsumer, OffsetAndMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.ByteArrayDeserializer

import scala.collection.mutable


/**
  * 负责从一个delay topic消费，然后发到相应的timer
  *
  *
  */
class MetaConsumerActor(config: MetaConsumerActorConfig) extends Actor with ActorLogging {

  private var consumer: MetaConsumer = _
  private val metaStorage = (config.metaStorageCreator)()
  private val expireTimeParser = config.expireTimeParser
  private val baseTopic = config.baseTopic
  private val timers = config.timers

  override def preStart(): Unit = {
    consumer = config.consumerCreator(config)
    super.preStart()
  }

  override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
    log.error(reason, message.fold("")("Error when processing " + _.toString))
    message.foreach { msg => self ! msg }
    super.preRestart(reason, message)
  }

  override def postStop(): Unit = {
    consumer.close()
    metaStorage.close()
    super.postStop()
  }

  private var pollCounter = 0L

  /**
    * 在启动这个actor以后，应该通过发送initOffsets指定它从何处开始poll。但是当这个actor重启时，不需要再指定initOffset
    */
  override def receive: Receive = {
    //如果是在seek之后的第一次poll失败，那么可能就不会commit之前seek到的offset。
    case "poll" =>
      val records = consumer.poll(1000)
      val metas = new mutable.ArrayBuffer[StoreMeta](records.count())
      if (!records.isEmpty) {
        val iterator = records.iterator()
        val currentClockTime = SystemTime.milliseconds
        val currentAbsTime = SystemTime.hiResClockMs
        while (iterator.hasNext) {
          val record = iterator.next()
          val expireMs = expireTimeParser.parse(record)
          val meta = StoreMeta(baseTopic, record.partition(), record.offset(), expireMs)
          metas += meta
          val partition = record.partition()
          val timer = timers(partition)
          timer.add(
            DelayMessageMeta.fromClockTime(
              record.offset(),
              expireMs,
              currentClockTime,
              currentAbsTime)
          )
        }
        metaStorage.store(metas)
        consumer.commit()
      }
      //offsets.retention.minutes: offsetsRetentionMs Offsets older than this retention period will be discarded.
      //so commit periodically
      if (metas.isEmpty && pollCounter % 300 == 0) {
        consumer.commitAsync()
      }
      self ! "poll"
      pollCounter += 1
    case Start(initOffsets) =>
      seek(initOffsets)
      sender ! Started
      self ! "poll"
  }

  private def seek(initOffsets: Map[TopicPartition, Long]): Unit = {
    val toBeCommitted = initOffsets.map {
      case (tp, offset) => (tp, new OffsetAndMetadata(offset, "in-seek"))
    }
    consumer.seek(scala.collection.JavaConversions.mapAsJavaMap(toBeCommitted))
  }
}

object MetaConsumerActor {

  case object Started

  case class Start(initOffsets: Map[TopicPartition, Long])

  def defaultConsumerCreator(config: MetaConsumerActorConfig) = {
    new MetaConsumer(
      baseTopic = config.baseTopic,
      consumer = new KafkaConsumer[Array[Byte], Array[Byte]](consumerConfig(config))
    )
  }

  private def consumerConfig(config: MetaConsumerActorConfig) = {
    val properties = new Properties()
    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
    properties.put(ConsumerConfig.GROUP_ID_CONFIG, GroupNames.metaConsumerGroup(config.baseTopic))
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetReset.earliest.value)
    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
    properties
  }
}


case class MetaConsumerActorConfig(baseTopic: String,
                                   partitionNum: Int,
                                   bootstrapServers: String,
                                   expireTimeParser: ExpireTimeParser,
                                   timers: Seq[MessageTimer],
                                   metaStorageCreator: () => MetaStorage,
//                                   messageCacheActor: Option[ActorRef],
//                                   cacheWarmMs: Long,
                                   consumerCreator: (MetaConsumerActorConfig) => MetaConsumer) {
  require(partitionNum == timers.size, "partition number be the same as timer number")
}
