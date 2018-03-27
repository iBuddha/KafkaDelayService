package kafka.delay.message.storage

import java.util

import com.google.common.primitives.Longs
import kafka.delay.message.storage.AbstractKafkaMetaStorage.MetaKey
import org.apache.kafka.common.serialization.{Deserializer, Serializer}


class MetaKeySerializer extends Serializer[MetaKey]{
  val maxAllowed = Long.MaxValue >> 1
  /**
    * 所以offset不能超过Long.MAX << 2, 也就是Long.MAX的四分之一。同时，最多只能有4个分区
    */
  override def serialize(topic: String, data: MetaKey): Array[Byte] = {
    if(data.offset > maxAllowed)
      throw new IllegalArgumentException(s"offset ${data.offset} exceeded max value $maxAllowed")
    if(data.partition >= 4)
      throw new IllegalArgumentException(s"partition id ${data.partition} exceeded max value 4")

    val shiftedPar = data.partition.toLong << 62
    val compacted = data.offset | shiftedPar
    Longs.toByteArray(compacted)
  }

  override def configure(map: util.Map[String, _], b: Boolean): Unit = {}
  override def close(): Unit = {}
}

class MetaKeyDeserializer extends Deserializer[MetaKey] {
  val mask = Long.MaxValue >>> 1
  override def configure(configs: util.Map[String, _], isKey: Boolean): Unit = {}

  override def close(): Unit = {}

  override def deserialize(topic: String, data: Array[Byte]): MetaKey = {
    val longVal = Longs.fromByteArray(data)
    val offset = longVal & mask
    val partition = longVal >>> 62
    MetaKey(partition.toInt, offset)
  }
}