package kafka.delay.message.actor.cached

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, OneForOneStrategy, Props, Terminated}
import kafka.delay.message.actor.MetaConsumerActor.{Start, Started}
import kafka.delay.message.actor._
import kafka.delay.message.actor.cached.TimerConsumer.TimerConsumerConfig
import kafka.delay.message.client.cache.{RecordCache, TimerBasedRecordCache}
import kafka.delay.message.client.parser.{KeyBasedExpireTimeParser, KeyBasedRecordExpireTimeParser}
import kafka.delay.message.client.{KafkaAdmin, KafkaClientCreator}
import kafka.delay.message.storage.BitmapKafkaMetaStorage
import kafka.delay.message.timer.PeekableMessageTimer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration._

class CachedTopicDelayService(baseTopic: String,
                        partitionNum: Int,
                        metaConsumerStartOffsetOpt: Option[Map[TopicPartition, Long]],
                        cacheController: ActorRef,
                        config: DelayServiceConfig)
  extends Actor with ActorLogging {

  import CachedTopicDelayService._

  private val requestTimeout = config.expireSendTimeoutMs
  private val ignoreExpireBeforeMs = config.ignoreExpiredMessageBeforeMs
  private val bootstrapServers = config.bootstrapServers

  private implicit val executionContext = context.dispatcher

  private var started = false

  private val expiredQueues: Array[BlockingQueue[DelayMessageMeta]] =
    (0 until partitionNum).map(_ => new LinkedBlockingQueue[DelayMessageMeta]()).toArray

  private val timers: Array[PeekableMessageTimer] =
    (0 until partitionNum).map(idx =>
      new PeekableMessageTimer(expiredQueues(idx), tickMs = config.tickMs, wheelSize = 1024)
    ).toArray

  private val expiredSenderActor = context.actorOf(
    Props(
      classOf[ExpiredSenderActor],
      bootstrapServers,
      ExpiredSenderActor.defaultProducerCreator
    ).withDispatcher(Dispatchers.ProducerDispatcher),
    s"expiredSender-$baseTopic")
  context.watch(expiredSenderActor)

  private var messageConsumers: Array[ActorRef] = null

  private var timerConsumers: Array[ActorRef] = null

  val consumerCreator = MetaConsumerActor.defaultConsumerCreator(_)
  private var metaConsumerActor: ActorRef = null

  private var checkTask: Option[Cancellable] = None

  override def preStart(): Unit = {
    checkTopicInfo()
    val metaConsumerStartOffset = recoverState(config.skipRestoreState)
    checkTask = Some(context.system.scheduler.scheduleOnce(60 seconds, self, TopicDelayService.CheckStarted))
    //consume from delay topic
    val metaConsumerConfig =
      new MetaConsumerActorConfig(
        baseTopic = this.baseTopic,
        partitionNum = this.partitionNum,
        bootstrapServers = this.bootstrapServers,
        expireTimeParser = new KeyBasedExpireTimeParser,
        timers = this.timers,
        metaStorageCreator(bootstrapServers, baseTopic),
        consumerCreator
      )
    metaConsumerActor = context.actorOf(
      Props(classOf[MetaConsumerActor], metaConsumerConfig)
        .withDispatcher(Dispatchers.MessageConsumerDispatcher),
      s"MetaConsumer-$baseTopic")
    context.watch(metaConsumerActor)
    //message consumer for expired message
    messageConsumers = {
      //      if(cacheController.isDefined) {
      (0 until partitionNum).map { partitionId =>
        context.actorOf(
          Props(
            new CachedMessageConsumerActor(
              baseTopic,
              partitionId,
              bootstrapServers,
              expiredSenderActor,
              cacheController,
              newRecordCache(),
              new NoCacheMessageConsumer(
                baseTopic,
                partitionId,
                bootstrapServers,
                KafkaClientCreator
              )
            )
          ).withDispatcher(Dispatchers.MessageConsumerDispatcher),
          s"message-consumer-$baseTopic-$partitionId"
        )
      }.toArray
    }
    messageConsumers.foreach(context.watch)
    //timer consumers to drive expired message consuming and sending
    val timerConsumerConfig = TimerConsumerConfig(
      null,
      expiredQueues(0),
      timers(0),
      messageConsumers(0),
      baseTopic,
      0,
      requestTimeout,
      config
    )

    timerConsumers =
      (0 until partitionNum).map { partitionId =>
        context.actorOf(
          Props(
            new TimerConsumer(
              timerConsumerConfig.copy(
                metaStorageCreator = metaStorageCreator(bootstrapServers, baseTopic),
                expiredMessageQueue = expiredQueues(partitionId),
                timer = timers(partitionId),
                messageConsumer = messageConsumers(partitionId),
                partitionId = partitionId
              )
            )
          ).withDispatcher("timer-consumer-dispatcher"),
          s"timer-consumer-$baseTopic-$partitionId")
      }.toArray

    timerConsumers.foreach(context.watch)

    metaConsumerActor ! Start(metaConsumerStartOffset)
    super.preStart()
  }

  override def postStop(): Unit = {
    log.warning("closing")
    checkTask.foreach(_.cancel())
    super.postStop()
  }

  override val supervisorStrategy = OneForOneStrategy(maxNrOfRetries = 10,
    withinTimeRange = 10 minutes) {
    case _: Exception => Stop
  }

  override def receive: Receive = {
    case Terminated(child) =>
      log.error("child: {} is terminated, so terminate self", child)
      context.stop(self)
    case Started =>
      started = true
      log.info("MetaConsumer started")
    case TopicDelayService.CheckStarted =>
      if (!started)
        throw new IllegalStateException("can't start within configured timeout")
      else
        log.info("TopicDelayService for {} has started", baseTopic)
  }

  private def recoverState(skipRecover: Boolean): Map[TopicPartition, Long] = {
    val delayTopicName = TopicMapper.getDelayTopicName(baseTopic)
    val latestCommittedDelayTopicOffset: Map[Int, Option[OffsetAndMetadata]] =
      KafkaAdmin.latestCommittedOffset(
        bootstrapServers,
        delayTopicName,
        partitionNum,
        GroupNames.metaConsumerGroup(baseTopic))
    //check meta consumer start offset configuration
    if (metaConsumerStartOffsetOpt.isDefined) {
      if (metaConsumerStartOffsetOpt.get.size != partitionNum)
        throw new IllegalArgumentException(
          "metaConsumerStartOffsetOpt should have same size with partition number," +
            s"currently partition number $partitionNum," +
            s" metaConsumerStartOffsetOpt size ${metaConsumerStartOffsetOpt.size}")
    } else {
      //then we use latest committed offset as meta consumer's start offset
      if (latestCommittedDelayTopicOffset.size != partitionNum) {
        throw new IllegalStateException(
          "latest meta consumer committed offset do not contain all partitions information," +
            s"current value is $latestCommittedDelayTopicOffset"
        )
      }
    }

    val ignoreOffsetUntil = latestCommittedDelayTopicOffset.map {
      //when no offset ever committed, assume meta consumer will consumer from beginning, so set it to  0
      case (partitionId, offsetOpt) => partitionId -> offsetOpt.map(_.offset).getOrElse(0L)
    }
    if(!skipRecover) {
      val kafkaStorage = new BitmapKafkaMetaStorage(bootstrapServers, baseTopic, SystemTime)
      kafkaStorage.restore(baseTopic, ignoreOffsetUntil, timers, ignoreExpireBeforeMs)
    }
    metaConsumerStartOffsetOpt.getOrElse(
      latestCommittedDelayTopicOffset.map {
        case (partitionId, metaOpt) => (new TopicPartition(delayTopicName, partitionId), metaOpt.get.offset())
      })
  }

  private def checkTopicInfo() = {
    val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
    val expireTopic = TopicMapper.getExpiredTopicName(baseTopic)
    val metaTopic = TopicMapper.getMetaTopicName(baseTopic)
    val topics = Seq(delayTopic, expireTopic, metaTopic)
    val topicInfos = KafkaAdmin.topicInfos(bootstrapServers, topics)

    def checkTopicInfo(topic: String): Unit = {
      if (!topicInfos.contains(topic))
        throw new KafkaException(s"$topic don't exists")
      else {
        val info = topicInfos(topic)
        if (info.size != partitionNum) {
          throw new KafkaException(s"$topic has incorrect partition number," +
            s" required $partitionNum," +
            s" actually ${info.size}")
        }
      }
    }

    topics.foreach(checkTopicInfo)
  }
}

object CachedTopicDelayService {

  case object CheckStarted

  val PreConsumeMaxRange = 8 * Time.MsPerSec * Time.SecsPerMin
  val RecordCacheExpireMs = 10 * Time.MsPerSec * Time.SecsPerMin
  val RecordCachePaddingMs = 2 * Time.MsPerSec
  val RecordCacheClearDuration = 5 * Time.MsPerSec * Time.SecsPerMin //cache里的数据如果已经过期，并且过期距当前时间大于此值。会被周期性清除

  def metaStorageCreator(bootstrapServers: String, baseTopic: String)() = {
    new BitmapKafkaMetaStorage(bootstrapServers, baseTopic, SystemTime)
  }

  def newRecordCache(): TimerBasedRecordCache = {
    new TimerBasedRecordCache(
      RecordCache.InitialRecordCacheSize,
      RecordCacheExpireMs,
      RecordCachePaddingMs,
      new KeyBasedRecordExpireTimeParser)
  }
}