package kafka.delay.message.actor.metrics

import akka.actor.{Actor, ActorLogging, Cancellable}
import kafka.delay.message.actor.request._
import org.apache.kafka.common.TopicPartition

import scala.concurrent.duration.FiniteDuration


/**
  * 用于授理各个cache申请内存的请求。它并不主动指定各个actor的cache大小，而是由cache来申请内存.
  *
  * 需要考虑到此actor接收到CacheSpaceRequest时，cache发出此请求时做出判断时依据的信息是否已过期。
  * 比如 cache -> CacheSpaceRequest, cache -> CacheSpaceRequest, controller -> SetCacheSizeRequest。
  * controller作出响应时根据的是第一个请求，
  * 因此应该忽略第二个请求。cache应该根据controller的响应，调整自己的大小。然后再发出请求
  *
  */
class CacheControllerActor(maxSize: Int,
                           miniSize: Int,
                           checkInterval: FiniteDuration) extends Actor with ActorLogging {

  import CacheControllerActor._

  private val controller: CacheController = new PooledCachedController(maxSize, miniSize)
  private var checkTask: Option[Cancellable] = None

  override def preStart(): Unit = {
    log.info("cache controller actor started")
    checkTask = Some(
      context.system.scheduler.schedule(
        checkInterval,
        checkInterval,
        self,
        CheckCacheState
      )(context.system.dispatcher)
    )
    super.preStart()
  }

  override def postStop(): Unit = {
    log.info("cache controller actor stopped")
    super.postStop()
  }

  override def receive = {
    case GetCacheControllerMetaRequest =>
      sender ! CacheControllerMetaResponse(maxSize, miniSize)
    case UpdateCacheStateRequest(remoteState) =>
      log.info("updating cache state {}", remoteState)
      onRequest(remoteState.version, remoteState.tp)(controller.update(remoteState))
    case CacheSpaceRequest(tp, actor, targetSize, point, version) =>
      log.info("received cache space request for {} of target size: {}  point {} version {}",
        tp, targetSize, point, version)
      onRequest(version, tp) {
        val result = controller.assign(version, tp, actor, targetSize, point)
        result.foreach {
          case (tp, currentState) =>
            controller.actorOf(tp).map(_ ! SetCacheSizeRequest(currentState.currentSize, currentState.version))
        }
      }
    case CheckCacheState =>
      controller.getCaches().foreach { cache =>
        cache.actor ! CheckCacheStateRequest
      }
  }


  private def sendCurrentSize(tp: TopicPartition): Unit = {
    val currentState = controller.getState(tp).get
    sender ! SetCacheSizeRequest(currentState.currentSize, currentState.version)
  }

  private def validRequest(requestVersion: Long, tp: TopicPartition): Boolean =
    controller.getState(tp).fold(true)(_.version <= requestVersion)

  private def onRequest(requestVersion: Long, tp: TopicPartition)(onSuccess: => Unit) = {
    if (validRequest(requestVersion, tp)) {
      onSuccess
    } else {
      sendCurrentSize(tp)
    }
  }
}

object CacheControllerActor {

  case object CheckCacheState

}
