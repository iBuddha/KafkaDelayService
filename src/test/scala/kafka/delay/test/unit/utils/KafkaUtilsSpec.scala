package kafka.delay.test.unit.utils

import com.google.common.primitives._
import kafka.delay.test.unit.kafka.KafkaUtils
import org.apache.commons.io.Charsets
import org.scalatest.{FlatSpecLike, Matchers}

class KafkaUtilsSpecextends extends FlatSpecLike with Matchers {

  "newConsumerRecord" should "work for String" in {
    val key = "hello"
    val value = "world"
    val record = KafkaUtils.newConsumerRecord("topic", 0, 0, key, value)
    new String(record.key(), Charsets.UTF_8) shouldEqual "hello"
    new String(record.value, Charsets.UTF_8) shouldEqual "world"
  }

  "newConsumerRecord" should "work for int" in {
    val key = 1
    val value = 2
    val record = KafkaUtils.newConsumerRecord("topic", 0, 0, key, value)
    Ints.fromByteArray(record.key()) shouldBe 1
    Ints.fromByteArray(record.value()) shouldBe 2
  }

  "newConsumerRecord" should "work for long" in {
    val key = 2L
    val value = 3L
    val record = KafkaUtils.newConsumerRecord("topic", 0, 0, key, value)
    Longs.fromByteArray(record.key()) shouldBe 2L
    Longs.fromByteArray(record.value()) shouldBe 3L
  }

  "newConsumerRecord" should "work for byte array" in {
    val key = "hello".getBytes(Charsets.UTF_8)
    val value = "world".getBytes(Charsets.UTF_8)
    val record = KafkaUtils.newConsumerRecord("topic", 0, 0, key, value)
    new String(record.key(), Charsets.UTF_8) shouldEqual "hello"
    new String(record.value, Charsets.UTF_8) shouldEqual "world"
  }
}
