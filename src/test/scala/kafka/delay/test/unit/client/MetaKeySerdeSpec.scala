package kafka.delay.test.unit.client

import kafka.delay.message.storage.AbstractKafkaMetaStorage.MetaKey
import kafka.delay.message.storage.{MetaKeyDeserializer, MetaKeySerializer}
import org.scalatest.{FlatSpec, Matchers}

class MetaKeySerdeSpec extends FlatSpec with Matchers {
  import MetaKeySerdeSpec._

  "MetaKeySerializer and MetaKeyDeserializer" should "serde normal value correctly" in {
    serdeTest(1, 1)
  }

  "MetaKeySerializer and MetaKeyDeserializer" should "serde edge value correctly" in {
    val maxAllowedLong = Long.MaxValue >> 1
    val minAllowedLong = 0L
    val maxAllowedPartitionId = 3
    val minAllowedPartitionId = 0
    serdeTest(maxAllowedPartitionId, maxAllowedLong)
    serdeTest(minAllowedPartitionId, maxAllowedLong)
    serdeTest(maxAllowedPartitionId, minAllowedLong)
    serdeTest(minAllowedPartitionId, minAllowedLong)
  }

  "MetaKeySerializer" should "throw Error when illegal arguments being used" in {
    val illegalPartitionId = 4
    val illegalOffset = Long.MaxValue - 1
    val serializer = new MetaKeySerializer
    assertThrows[IllegalArgumentException](serializer.serialize("", MetaKey(illegalPartitionId, 0L)))
    assertThrows[IllegalArgumentException](serializer.serialize("", MetaKey(0, illegalOffset)))
  }
}

object MetaKeySerdeSpec{
  def serdeTest(partition: Int, offset: Long): Unit = {
    val metaKey = MetaKey(partition, offset)
    val serializer = new MetaKeySerializer
    val deserializer = new MetaKeyDeserializer

    val serialized = serializer.serialize(null, metaKey)
    val deserialized = deserializer.deserialize(null, serialized)
    assert(deserialized.partition == partition)
    assert(deserialized.offset == offset)
  }
}
