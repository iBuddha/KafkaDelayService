package kafka.delay.test.unit.client

import com.google.common.primitives.Longs
import kafka.delay.message.client.parser.KeyBasedExpireTimeParser
import kafka.delay.message.utils.SystemTime
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.scalatest.{FlatSpec, Matchers}

class KeyBasedExpireTimeParserSpec extends FlatSpec with Matchers{
  "KeyBasedExpireTimeParser" should "correctly parse record" in {
    val currentTime = SystemTime.milliseconds
    val record = new ConsumerRecord[Array[Byte], Array[Byte]]("test-topic", 0, 0, Longs.toByteArray(currentTime), "".getBytes())
    val parser = new KeyBasedExpireTimeParser()
    parser.parse(record) shouldBe currentTime
  }
}
