package kafka.delay.message.utils

import java.util.Properties

import kafka.delay.message.client.OffsetReset
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.ByteArrayDeserializer


/**
  * 把KafkaConsumer作为一个分布式锁使用
  *
  * @param topic
  * @param groupId
  */
class ConsumerAsLock(topic: String, groupId: String) {
//  def tryLock(): Boolean = {
//
//  }
//
//  private def getConsumer(): KafkaConsumer[Array[Byte], Array[Byte]] = {
//    val properties = new Properties()
//    properties.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service")
//    properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
//    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, OffsetReset.earliest.value)
//    properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
//    properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.bootstrapServers)
//    properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
//    properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
//    properties
//  }
//  }
}
