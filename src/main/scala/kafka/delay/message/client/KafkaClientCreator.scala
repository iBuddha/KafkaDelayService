package kafka.delay.message.client

import java.util.Properties

import org.apache.kafka.clients.consumer.{Consumer, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, Producer}

object KafkaClientCreator extends ClientCreator {
  override def getConsumer[K, V](config: Properties): Consumer[K, V] = {
    new KafkaConsumer[K, V](config)
  }

  override def getProducer[K, V](config: Properties): Producer[K, V] = {
    new KafkaProducer[K,V](config)
  }
}
