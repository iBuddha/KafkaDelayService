package kafka.delay.test.unit.actor

import akka.actor.SupervisorStrategy.Stop
import akka.actor.{Actor, ActorRef, ActorSystem, OneForOneStrategy, Props, SupervisorStrategy}

import scala.concurrent.duration._

/**
  * 当child抛出异常时，把异常作为消息转发给probe actor
  * 为了规避ScalaMock的信息抛不出来的问题
  *
  * @param name
  */
class ExceptionProxy(childProps: Props, name: String, probe: ActorRef) extends Actor {

  override def supervisorStrategy: SupervisorStrategy =
    OneForOneStrategy(maxNrOfRetries = 0,
                      withinTimeRange = 1 minutes) {
      case exception: Throwable =>
        probe ! exception
        Stop
    }

  val child = context.actorOf(childProps, name)

  override def receive: Receive = {
    case msg =>
      if (sender == child)
        context.parent ! msg
      else
        child forward msg
  }
}

trait WithExceptionProxy {
  def system: ActorSystem

  def newProxy(props: Props, name: String, probe: ActorRef) = {
    system.actorOf(Props(new ExceptionProxy(props, name, probe)), s"exception-proxy-for-$name")
  }
}
