package kafka.delay.test.unit.client.cache

import kafka.delay.message.client.cache.{RecordSizeSampler, RecordWheelCache}
import kafka.delay.message.client.cache.TimerBasedRecordCache.Element
import kafka.delay.test.unit.kafka.KafkaUtils
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FlatSpec, Matchers}

class RecordWheelCacheSpec extends FlatSpec with Matchers {

  "record wheel cache" should "deny element" in {
    implicit val tp = new TopicPartition("test", 0)
    val records = (0 to 100).map { i => KafkaUtils.newRecordWithSize(i, 10, 10) }
    val now = 0L
    val wheel = new RecordWheelCache(tickMs = 10, wheelSize = 5, startMs = 14)
    records.foreach { r =>
      val expireMs = now + r.offset()
      val inserted = wheel.put(r, RecordSizeSampler.bytes(r), expireMs)
      if(expireMs <= 9 || expireMs >= 60){
        inserted shouldBe None
      } else {
        inserted.get.cacheElement.record shouldBe r
      }
    }
  }

  "record wheel cache" should "expire" in {
    implicit val tp = new TopicPartition("test", 0)
    val records = (0 to 100).map { i => KafkaUtils.newRecordWithSize(i, 10, 10) }
    val now = 0L
    val wheel = new RecordWheelCache(tickMs = 10, wheelSize = 5, startMs = 14)
    records.foreach { r =>
      val expireMs = now + r.offset()
      wheel.put(r, RecordSizeSampler.bytes(r), expireMs)
    }

    val expired = wheel.advanceAndClearExpired(20)
    expired.size shouldBe 20
    expired.map(_._2.cacheElement.record.offset()).toSeq.sorted shouldEqual (10 to 29).toSeq
    wheel.minMs shouldBe 30
    wheel.maxMs shouldBe 79
  }

  "record wheel cache" should "clear bucket after expired" in {
    implicit val tp = new TopicPartition("test", 0)
    val records = (0 until 100).map { i => KafkaUtils.newRecordWithSize(i, 10, 10) }
    val now = 0L
    val wheel = new RecordWheelCache(tickMs = 10, wheelSize = 5, startMs = 14)
    records.foreach { r =>
      val expireMs = now + r.offset()
      wheel.put(r, RecordSizeSampler.bytes(r), expireMs)
    }

    wheel.advanceAndClearExpired(99)
    wheel.minMs shouldBe 100
    wheel.maxMs shouldBe 149
    (100 until 200)
      .map { i => KafkaUtils.newRecordWithSize(i, 10, 10) }
      .foreach{r =>
        val expireMs = now + r.offset()
        wheel.put(r, RecordSizeSampler.bytes(r), expireMs)
      }
    wheel.advanceAndClearExpired(100).size shouldBe 10
  }


  "record wheel cache" should "release" in {
    implicit val tp = new TopicPartition("test", 0)
    val records = (0 until 100).map { i => KafkaUtils.newRecordWithSize(i, 10, 10) }
    val now = 0L
    val wheel = new RecordWheelCache(tickMs = 10, wheelSize = 5, startMs = 14)
    records.foreach { r =>
      val expireMs = now + r.offset()
      wheel.put(r, RecordSizeSampler.bytes(r), expireMs)
    }

    var released = wheel.releaseOneBucket()
    released.map(_._1).toList.sorted shouldEqual (50L until 60).toList
    released = wheel.releaseOneBucket()
    released.map(_._1).toList.sorted shouldEqual (40L until 50).toList
    wheel.releaseOneBucket()
    wheel.releaseOneBucket()
    wheel.releaseOneBucket()
    released = wheel.releaseOneBucket()
    released.size shouldBe 0

  }
}
