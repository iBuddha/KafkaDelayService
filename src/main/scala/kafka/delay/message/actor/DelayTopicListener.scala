package kafka.delay.message.actor

import akka.actor.{Actor, ActorLogging, ActorRef, Cancellable}
import kafka.delay.message.actor.DelayServiceActor.BaseTopics
import kafka.delay.message.client.KafkaAdmin

import scala.concurrent.duration._

/**
  * 它周期性地获取topic的列表，凡是满足delay topic的命名规范的topic会被筛选出来，发送给delay service。
  * 这种topic是指其名命满足TopicMapper里的约定，并且有同样多的partition的topic
  */
class DelayTopicListener(delayServiceActor: ActorRef,
                         checkInterval: FiniteDuration,
                         bootstrapServers: String) extends Actor with ActorLogging {

  import DelayTopicListener._

  private implicit val executionContext = context.system.dispatcher
  var scheduledCheck: Option[Cancellable] = None
  var latestKnownTopics: Option[Set[String]] = None

  override def preStart(): Unit = {
    scheduledCheck = Some(context.system.scheduler.scheduleOnce(initDelay, self, Check))
    super.preStart()
    log.info("DelayTopicListener started")
  }

  override def postStop(): Unit = {
    scheduledCheck.foreach(_.cancel())
    super.postStop()
    log.info("DelayTopicListener stopped")
  }

  override def receive = {
    case Check => try {
      log.debug("Checking new topics")
      scheduledCheck = None
      import kafka.delay.message.utils.TopicMapper._
      val topics = KafkaAdmin.topics(bootstrapServers)
      val topicGroups = topics.map {
        case (topicName, partitionInfo) => (topicName, getBaseName(topicName)) -> partitionInfo
      }.filterKeys(_._2.isDefined)
        .groupBy(_._1._2.get)
      val delayTopics = topicGroups
        .filter(_._2.size == 3)
        .keySet

      if (latestKnownTopics.isEmpty) {
        latestKnownTopics = Some(delayTopics)
        log.info(s"Current delay topics: {}", delayTopics)
      } else {
        val diff = latestKnownTopics.get diff delayTopics
        if (!diff.isEmpty) {
          log.info("latest known delay topics {}, current detected {}", latestKnownTopics.get, delayTopics)
        }
        latestKnownTopics = Some(delayTopics)
      }

      log.debug(s"Current delay topics: {}", delayTopics)
      delayServiceActor ! BaseTopics(delayTopics)
    } catch {
      case e: Exception =>
        log.error(e, "Failed to fetch topic info")
    } finally {
      scheduledCheck = Some(context.system.scheduler.scheduleOnce(checkInterval, self, Check))
    }
  }
}

object DelayTopicListener {

  val initDelay = 3 seconds

  case object Check

}
