package kafka.delay.message.client.parser

import com.google.common.primitives.Longs
import org.apache.kafka.clients.consumer.ConsumerRecord

trait RecordExpireTimeParser {
  def getExpireMs(record: ConsumerRecord[Array[Byte], Array[Byte]]): Long
}

class KeyBasedRecordExpireTimeParser extends RecordExpireTimeParser {
  override def getExpireMs(record: ConsumerRecord[Array[Byte], Array[Byte]]) = {
    Longs.fromByteArray(record.key())
  }
}