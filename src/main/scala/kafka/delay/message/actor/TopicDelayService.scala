package kafka.delay.message.actor

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable, OneForOneStrategy, Props, Terminated}
import kafka.delay.message.actor.MetaConsumerActor.{Start, Started}
import kafka.delay.message.actor.NonAskTimerConsumer.NonAskTimerConsumerConfig
import kafka.delay.message.client.cache.{RecordCache, ScalableRecordCache, TimerBasedRecordCache}
import kafka.delay.message.client.parser.{KeyBasedExpireTimeParser, KeyBasedRecordExpireTimeParser}
import kafka.delay.message.client.{KafkaAdmin, KafkaClientCreator, MessageConsumer}
import kafka.delay.message.storage.{BitmapKafkaMetaStorage, MetaStorage}
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.timer.{MessageTimer, PeekableMessageTimer, SystemMessageTimer}
import kafka.delay.message.utils._
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.{KafkaException, TopicPartition}

import scala.concurrent.duration._

/**
  * TODO: 使得对于同一个baseTopic的多个TopicDelayService能够共同工作。
  * 这需要使得当已有其它TopicDelayService工作时，此TopicDelayService处理等待状态。
  * 此时，需要注意，restore方法必须在此service成为master时，才被调用。当成为slave时，它需要消空当前状态，包括当前的timer
  *
  *
  * 负责单独一个topic的delay服务
  * 包括：
  * 1. 从delay topic消费消息，发给timer
  * 2. 从timer拉取超时消息的元数据，从Kafka中获取超时消息的本体然后发给expired topic
  *
  * 它生成四类actor:
  * 1. ExpiredSenderActor 用于发送已经超时的消息，它是对KafkaProducer的一个简单封装
  * 2. MetaConsumer: 负责消费用户提交延迟消息的topic，从中取出delay相关的元数据，插入timer
  * 3. TimerConsumer: 从Timer poll出来超时的消息元数据，发送给MessageConsumer来完成后续处理，当处理失败时，它负责重试。
  * 3. MessageConsumer: 根据已delay完毕消息的元数据(MetaBatch)从Kafka中消费数据，然后发给ExpiredSender
  *
  * 其中的任何一个子actor停止，这个actor都要停止运行。
  *
  * @param baseTopic
  * @param partitionNum               partition的数量，所有相关topic的partition数量需要一致
  * @param metaConsumerStartOffsetOpt 如果指定，meta consumer就会从这个offset开始消费；如果没有指定,
  *                                   就使用consumer的latest committed offset。但是如果没有latest offset，就应该终止启动。
  *                                   这样是为了，如果consumer的 latest committed offset被清除了，可以手动指定。因为如果不能正确地
  *                                   确定meta consumer的start offset，可能会导致消息丢失或重复。
  */
class TopicDelayService(baseTopic: String,
                        partitionNum: Int,
                        metaConsumerStartOffsetOpt: Option[Map[TopicPartition, Long]],
                        cacheController: Option[ActorRef],
                        config: DelayServiceConfig)
  extends Actor with ActorLogging {

  import TopicDelayService._

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
    val metaConsumerStartOffset = recoverState()
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
            new NonAskMessageConsumerActor(
              baseTopic,
              partitionId,
              bootstrapServers,
              expiredSenderActor,
              cacheController,
              new MessageConsumer(
                baseTopic,
                partitionId,
                bootstrapServers,
                KafkaClientCreator,
                if (cacheController.isDefined) Some(newRecordCache()) else None
              )
            )
          ).withDispatcher(Dispatchers.MessageConsumerDispatcher),
          s"message-consumer-$baseTopic-$partitionId"
        )
      }.toArray
    }
    messageConsumers.foreach(context.watch)
    //timer consumers to drive expired message consuming and sending
    val timerConsumerConfig = NonAskTimerConsumerConfig(
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
            new NonAskTimerConsumer(
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

  private def recoverState(): Map[TopicPartition, Long] = {
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

    val kafkaStorage = new BitmapKafkaMetaStorage(bootstrapServers, baseTopic, SystemTime)
    kafkaStorage.restore(baseTopic, ignoreOffsetUntil, timers, ignoreExpireBeforeMs)
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

object TopicDelayService {

  case object CheckStarted

  val RecordCacheExpireMs = Time.MsPerHour
  val RecordCachePaddingMs = 2 * Time.MsPerSec
  val RecordCacheClearDuration = 5 * Time.MsPerSec * Time.SecsPerMin //cache里的数据如果已经过期，并且过期距当前时间大于此值。会被周期性清除

  def metaStorageCreator(bootstrapServers: String, baseTopic: String)() = {
      new BitmapKafkaMetaStorage(bootstrapServers, baseTopic, SystemTime)
  }

  def newRecordCache(): RecordCache = {
    new TimerBasedRecordCache(
      RecordCache.InitialRecordCacheSize,
      RecordCacheExpireMs,
      RecordCachePaddingMs,
      new KeyBasedRecordExpireTimeParser)
  }
}