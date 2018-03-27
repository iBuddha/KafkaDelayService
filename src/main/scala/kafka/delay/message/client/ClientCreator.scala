package kafka.delay.message.client

import java.util.Properties

import kafka.utils.threadsafe
import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.clients.producer.Producer

/**
  * to help test
  */
@threadsafe
trait ClientCreator {
  def getConsumer[K,V](config: Properties): Consumer[K,V]
  def getProducer[K, V](config: Properties): Producer[K,V]
}
