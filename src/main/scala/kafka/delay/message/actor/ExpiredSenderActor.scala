package kafka.delay.message.actor

import akka.actor.{Actor, ActorLogging}
import kafka.delay.message.actor.request.{RecordsSendRequest, RecordsSendResponse}
import kafka.delay.message.client.{MessageProducer, MessageProducerImpl}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.util.Try

class ExpiredSenderActor(bootstrapServers: String,
                         producerCreator: String => MessageProducer) extends Actor with ActorLogging {

  private var producer: MessageProducer = _

  private val closeTimeout: Long = 10 * 1000

  override def preStart(): Unit = {
    producer = producerCreator(bootstrapServers)
    super.preStart()
  }

  override def postStop(): Unit = {
    producer.close(closeTimeout)
    super.postStop()
  }

  override def receive: Receive = {
    case RecordsSendRequest(consumeResult, targetTopic, requestId) =>
//      log.debug("Sending {} records", consumeResult.size)
      val sendResults: Seq[Try[RecordMetadata]] = producer.send(consumeResult, targetTopic).map { r =>
        Try {
          r.get()
        }
      }
      sender ! RecordsSendResponse(consumeResult, sendResults, requestId)
  }
}

object ExpiredSenderActor {

  val defaultProducerCreator: String => MessageProducer =
    (bootstrapServers) => {
      new MessageProducerImpl(bootstrapServers)
    }
}