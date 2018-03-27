package kafka.delay.message

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import com.typesafe.config.ConfigFactory
import kafka.delay.message.actor.{DelayServiceActor, DelayTopicListener}
import kafka.delay.message.utils.DelayServiceConfig
import org.slf4j.LoggerFactory

import scala.concurrent.Await
import scala.concurrent.duration.{Duration, FiniteDuration}

object Main extends App {

  val logger = LoggerFactory.getLogger(Main.getClass)
  logger.info("starting DelayService")

  val config = ConfigFactory.load()
  val delayServiceConfig = DelayServiceConfig(config)

  val actorSystem = ActorSystem("DelayService")
  val delayServiceActor = actorSystem.actorOf(
    Props(
      classOf[DelayServiceActor],
      delayServiceConfig
    ),
    "delay-service")

  val checkIntervalConfig = delayServiceConfig.newTopicCheckInterval
  val checkInterval = FiniteDuration(checkIntervalConfig.getSeconds, TimeUnit.SECONDS)

  val newTopicListener = actorSystem.actorOf(
    Props(
      classOf[DelayTopicListener],
      delayServiceActor,
      checkInterval,
      delayServiceConfig.bootstrapServers
    ).withDispatcher("topic-listener-dispatcher"),
    "delay-topic-listener")
  logger.info("waiting for actor system to stop")
  Await.ready(actorSystem.whenTerminated, Duration.Inf)
}
