package kafka.delay.test.unit.client.cache

import com.google.common.primitives.Longs
import kafka.delay.message.client.cache.{RecordSizeSampler, ScalableRecordCache}
import kafka.delay.message.client.parser.KeyBasedRecordExpireTimeParser
import kafka.delay.message.timer.meta.ArrayOffsetBatch
import kafka.delay.test.unit.kafka.KafkaUtils
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FlatSpec, Matchers}
import ScalableRecordCacheSpec._

class ScalableRecordCacheSpec extends FlatSpec with Matchers {
  "record cache" should "refuse to add record exceed max size" in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser)

    val now = System.currentTimeMillis()

    val record = newRecord(0, 1025, System.currentTimeMillis())
    assert(RecordSizeSampler.bytes(record) == 1025)
    cache += (record, now)
    cache.getSizeInBytes() shouldBe 0

  }

  "record cache" should "be able to add records" in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser)

    val now = System.currentTimeMillis()
    cache += (newRecord(0, 200, now + 100), now)
    cache += (newRecord(0, 201, now + 100), now)
    cache.get(0).isDefined shouldBe true
    cache.getSizeInBytes() shouldBe 200
    cache += (newRecord(1, 200, now + 200), now)
    cache += (newRecord(2, 300, now + 300), now)
    cache.get(1).isDefined shouldBe true
    cache.get(2).isDefined shouldBe true
    cache.get(3).isDefined shouldBe false
  }

  "record cache" should "deny add record already expired" in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser)

    val now = System.currentTimeMillis()
    cache += (newRecord(0, 200, now - 1), now)
    cache.getSizeInBytes() shouldBe 0
    cache += (newRecord(0, 200, now), now)
    cache.get(0).isDefined shouldBe false
  }

  "record cache" should "clear already expired records first when adding records" in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser)

    var now = System.currentTimeMillis()
    (cache += (newRecord(0, 200, now + 1), now)) shouldBe false
    (cache += (newRecord(1, 200, now + 2), now)) shouldBe false
    (cache += (newRecord(2, 300, now + 4), now)) shouldBe false
    now += 2
    (cache += (newRecord(3, 800, now + 1), now)) shouldBe true
    cache.contains(0) shouldBe false
    cache.contains(1) shouldBe false
    cache.contains(2) shouldBe false
    cache.contains(3) shouldBe true
  }

  "record cache" should "tell not enough space " in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser)

    val now = System.currentTimeMillis()
    (cache += (newRecord(0, 200, now + 1), now)) shouldBe false
    (cache += (newRecord(1, 200, now + 2), now)) shouldBe false
    (cache += (newRecord(2, 300, now + 4), now)) shouldBe false
    (cache += (newRecord(3, 1000, now + 3), now)) shouldBe true
    (cache += (newRecord(4, 1000, now + 4), now)) shouldBe true
    (cache += (newRecord(5, 1024, now + 4), now)) shouldBe true
    (cache += (newRecord(6, 1025, now + 4), now)) shouldBe true
  }

  "record cache" should "remove" in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser)

    val now = System.currentTimeMillis()
    cache += (newRecord(0, 200, now + 1), now)
    cache += (newRecord(1, 200, now + 2), now)
    cache += (newRecord(2, 200, now + 4), now)
    cache -= 0
    cache.contains(0) shouldBe false
    cache.getSizeInBytes() shouldBe 400
  }

  "record cache" should "resize" in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser)

    val now = System.currentTimeMillis()
    cache += (newRecord(0, 200, now + 1), now)
    cache += (newRecord(1, 200, now + 2), now)
    cache += (newRecord(2, 200, now + 4), now)
    cache += (newRecord(3, 200, now + 4), now)
    cache.getSizeInBytes() shouldBe 800
    cache.resize(400, now, 1)
    cache.getSizeInBytes() shouldBe 400
    cache.contains(0) shouldBe false
    cache.contains(1) shouldBe false
    cache.contains(2) shouldBe true
    cache.contains(3) shouldBe true
  }

  "RecordCache" should "create new batch when get partial result" in {
    val cache = new ScalableRecordCache(
      maxBytes = 1024 * 100,
      maxExpireMs = 10000,
      paddingTime = 0,
      expireMsParser = expireMsParser
    )
    val now = System.currentTimeMillis()
    cache += (newRecord(0, 200, now + 1), now)
    cache += (newRecord(1, 200, now + 2), now)
    cache += (newRecord(2, 200, now + 4), now)
    cache += (newRecord(3, 200, now + 4), now)
    cache += (newRecord(4, 200, now + 4), now)

    // batch all cached
    var batch = new ArrayOffsetBatch(Array(0L, 1L, 2))
    var cachedAndNot = cache.get(batch)
    cachedAndNot.cached.map(_.offset()) shouldEqual Seq(0, 1, 2L)
    cachedAndNot.nonCached shouldBe None

    //batch no one cached
    batch = new ArrayOffsetBatch(Array(8, 19))
    cachedAndNot = cache.get(batch)
    cachedAndNot.cached shouldBe None
    cachedAndNot.nonCached.get shouldEqual batch

    //batch cached from head to tail

    batch  = new ArrayOffsetBatch(Array(1, 2, 3))
    cachedAndNot = cache.get(batch)
    cachedAndNot.cached.map(_.offset()) shouldEqual batch
    cachedAndNot.nonCached shouldBe None

    //batch cached from head but tail is not cached
    batch = new ArrayOffsetBatch(Array(0, 1, 2, 3, 4, 5, 6))
    cachedAndNot = cache.get(batch)
    cachedAndNot.cached.map(_.offset()) shouldEqual Array(0, 1, 2, 3, 4)
    cachedAndNot.nonCached.get.toSeq shouldEqual new ArrayOffsetBatch(Array(5, 6))

    // batch head is not cached
    cache -= 0L
    batch = new ArrayOffsetBatch(Array(0, 1, 2, 3))
    cachedAndNot = cache.get(batch)
    cachedAndNot.cached shouldBe None
    cachedAndNot.nonCached.get shouldEqual batch

    // batch is empty
    batch = new ArrayOffsetBatch(Array.emptyLongArray)
    cachedAndNot = cache.get(batch)
    cachedAndNot.cached shouldBe None
    cachedAndNot.nonCached.get shouldEqual batch
  }
}

object ScalableRecordCacheSpec {
  val expireMsParser = new KeyBasedRecordExpireTimeParser
  implicit val tp = new TopicPartition("foo", 0)

  def newRecord(offset: Long, size: Int, expireMs: Long): ConsumerRecord[Array[Byte], Array[Byte]] = {
    val serializedKey = Longs.toByteArray(expireMs)
    val paddingSize = size - RecordSizeSampler.FixedSize - serializedKey.length
    KafkaUtils.newRecordWithSize(offset, serializedKey, 0, new Array[Byte](paddingSize), paddingSize)
  }
}