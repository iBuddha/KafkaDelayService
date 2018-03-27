package kafka.delay.message.client.parser

import com.google.common.primitives.Longs
import org.apache.kafka.clients.consumer.ConsumerRecord

class KeyBasedExpireTimeParser extends ExpireTimeParser{
  override def parse(record: ConsumerRecord[Array[Byte], Array[Byte]]): Long = Longs.fromByteArray(record.key())

//  override def createConsumer(config: Properties): Consumer[Long, Array[Byte]] = {
//    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getCanonicalName)
//    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[ByteArrayDeserializer].getCanonicalName)
//    new KafkaConsumer[Long, Array[Byte]](config)
//  }
//
//  override def parseFromBytes(record: ConsumerRecord[Array[Byte], ConsumerRecord[Array[Byte]]]) = {
//    Longs.fromByteArray(record.key())
//  }
}
