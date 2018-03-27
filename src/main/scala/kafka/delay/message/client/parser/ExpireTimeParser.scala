package kafka.delay.message.client.parser

import java.util.Properties

import kafka.utils.threadsafe
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}

@threadsafe
trait ExpireTimeParser {
  def parse(record: ConsumerRecord[Array[Byte], Array[Byte]]): Long
}
