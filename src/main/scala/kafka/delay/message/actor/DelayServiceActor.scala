package kafka.delay.message.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern.{Backoff, BackoffSupervisor}
import kafka.delay.message.actor.cached.CachedTopicDelayService
import kafka.delay.message.actor.metrics.CacheControllerActor
import kafka.delay.message.client.KafkaAdmin
import kafka.delay.message.utils.{DelayServiceConfig, GroupNames, TopicMapper}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable

/**
  * TODO: supervisor当前的supervisor strategy 并不会因为actor initial failure而重启。应该使用特定的supervisor strategy以使
  * 得它会重启
  */
class DelayServiceActor(config: DelayServiceConfig)
  extends Actor with ActorLogging {

  import DelayServiceActor._

  private val topicDelayServiceSupervisors: mutable.Map[String, ActorRef] = mutable.Map.empty
  private val bootstrapServers = config.bootstrapServers
  private var cacheController: Option[ActorRef] = None

  override def preStart(): Unit = {
    log.info("DelayService started")
    if (config.consumerCacheMaxBytes > 0) {
      log.info("record cache enabled")
      cacheController = Some(
        context.actorOf(
          Props(
            new CacheControllerActor(
              maxSize = config.consumerCacheMaxBytes,
              miniSize = DelayServiceConfig.MiniCacheSize,
              checkInterval = DelayServiceConfig.CacheCheckInterval)),
          "cache-controller")
      )
    }
    super.preStart()
  }

  override def postStop(): Unit = {
    log.info("DelayService stopped")
    super.postStop()
  }

  override def receive: Receive = {
    case BaseTopics(baseNames) =>
      log.debug("Received topic names: {}", baseNames.mkString(","))
      val newTopics = baseNames -- topicDelayServiceSupervisors.keySet
      newTopics.foreach { baseName =>
        log.info("Received new topic with base name: {}", baseName)
      }
      newTopics.foreach { baseName =>
        addService(baseName)
      }
      val deadTopics = topicDelayServiceSupervisors.keySet -- baseNames
      deadTopics.foreach { topic =>
        stopService(topic)
      }

    case Terminated(child) =>
      log.warning("{} terminated", child)
  }

  private def addService(baseTopicName: String): Unit = {
    log.info("Adding delay service for base topic: {}", baseTopicName)
    val legitimate = ensureTopics(baseTopicName)
    if (!legitimate)
      log.info("Skip adding delay service for {}, because related topics aren't all legitimate", baseTopicName)
    else {
      //where to start consuming delay topic after restart
      var metaConsumerStartOffset: Map[TopicPartition, Long] = Map.empty
      /**
        * if meta topic is empty, means there are no previous messages processed, so start from begin offset of
        * delay topic, else find find meta consumer's latest committed offset, else throw an exception
        */
      val metaTopicName = TopicMapper.getMetaTopicName(baseTopicName)
      val endOffsetOfMetaTopic = KafkaAdmin.endOffsets(bootstrapServers, metaTopicName)
      val partitionNumber = KafkaAdmin.topicInfo(bootstrapServers, metaTopicName).get.size
      log.info("topic {} has {} partitions", baseTopicName, partitionNumber)
      val delayTopicName = TopicMapper.getDelayTopicName(baseTopicName)
      if (endOffsetOfMetaTopic.forall(_._2 == 0)) {
        log.info("Use 0 for meta consumer of {} as start offset because meta topic is empty", baseTopicName)
        //meta topic is empty
        metaConsumerStartOffset =
          (0 until partitionNumber).map { partitionId: Int =>
            (new TopicPartition(delayTopicName, partitionId), 0L)
          }.toMap
      } else {
        val metaConsumerGroup = GroupNames.metaConsumerGroup(baseTopicName)
        val latestCommitted =
          KafkaAdmin.latestCommittedOffset(
            bootstrapServers,
            delayTopicName,
            partitionNumber,
            metaConsumerGroup)
        if (!latestCommitted.forall(_._2.isDefined)) {
          log.error(
            "Meta topic for {} isn't empty," +
              s" but meantime can't get latest committed offset for $delayTopicName with group $metaConsumerGroup," +
              "so skip add delay service for this topic, please use external tools to set correct start offset for" +
              " this topic", baseTopicName)
        } else {
          metaConsumerStartOffset = latestCommitted.map {
            case (partitionId, partitionInfoOpts) =>
              (new TopicPartition(delayTopicName, partitionId), partitionInfoOpts.get.offset())
          }

          log.info(s"Use latest committed offset of {} with group name {}",
            delayTopicName,
            metaConsumerGroup)
          metaConsumerStartOffset.map {
            case (tp, offset) => log.info("last committed offset for {} is {}", tp.toString, offset)
          }
        }
      }


      if (metaConsumerStartOffset.isEmpty) {
        log.info(s"Can't determine meta consumer start offset, so skip adding delay service for $baseTopicName")
      } else {
        log.info(s"Chosen meta consumer start offsets of $metaConsumerStartOffset")
        val topicDelayServiceProps = Props(
          new CachedTopicDelayService(
            baseTopicName,
            partitionNumber,
            Some(metaConsumerStartOffset),
            cacheController.get,
            config)
        )
        val topicDelayServiceSupervisorProps = getTopicDelayServiceSupervisorProps(
          topicDelayServiceProps,
          baseTopicName)
        val topicDelayServiceSupervisor = context.actorOf(topicDelayServiceSupervisorProps,
          s"supervisor-$baseTopicName")
        topicDelayServiceSupervisors.put(baseTopicName, topicDelayServiceSupervisor)
        context.watch(topicDelayServiceSupervisor)
        log.info("Added delay service for {}", baseTopicName)
      }
    }
  }

  private def stopService(baseTopic: String): Unit = {
    log.warning("topic {} no more qualified as delay topic, stop related service", baseTopic)
    val service = topicDelayServiceSupervisors.remove(baseTopic).get
    context.stop(service)
  }

  /**
    * 如果所有相关topic: (delay topic, expire topic, meta topic)都存在，并且partition数量相等，就返回true
    * 否则返回false
    */
  private def ensureTopics(baseTopic: String): Boolean = {
    val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
    val expireTopic = TopicMapper.getExpiredTopicName(baseTopic)
    val metaTopic = TopicMapper.getMetaTopicName(baseTopic)
    val infos = KafkaAdmin.topicInfos(bootstrapServers, Seq(delayTopic, expireTopic, metaTopic))
    var isCorrect = true
    (Set(delayTopic, expireTopic, metaTopic) -- infos.keySet).foreach { t =>
      log.error(s"$t doesn't exists for base topic: $baseTopic")
      isCorrect = false
    }
    if (isCorrect) {
      infos.foreach {
        case (topic, partitionInfos) =>
          if (partitionInfos.size != infos.head._2.size) {
            log.error(s"$baseTopic's related topics don't have the same partition number")
            isCorrect = false
          }
      }
    }
    isCorrect
  }
}

object DelayServiceActor {

  import scala.concurrent.duration._

  /**
    * @param baseNames 当前需要的delay topic列表。这里的名字是base name
    */
  case class BaseTopics(baseNames: Set[String])

  def getTopicDelayServiceSupervisorProps(childProps: Props, actorName: String): Props =
    BackoffSupervisor.props(
      Backoff.onFailure(
        childProps,
        childName = actorName,
        minBackoff = 3.seconds,
        maxBackoff = 30.seconds,
        randomFactor = 0.2 // adds 20% "noise" to vary the intervals slightly
      ))

}
