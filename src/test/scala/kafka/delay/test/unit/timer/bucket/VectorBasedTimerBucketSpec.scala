package kafka.delay.test.unit.timer.bucket

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong

import kafka.delay.message.timer.bucket.VectorBasedTimerBucket
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.SystemTime
import org.scalatest.{FlatSpec, Matchers}

class VectorBasedTimerBucketSpec extends FlatSpec with Matchers {
  "VectorBasedMetaBufferSpec" should "setExpireTime correctly" in {
    val buffer = new VectorBasedTimerBucket(new AtomicLong(0))
    assert(buffer.setExpiration(10) == true)
    assert(buffer.setExpiration(10) == false)
    assert(buffer.getExpiration() == 10L)
    buffer.setExpiration(100)
    assert(buffer.getExpiration() == 100L)
  }

  "VectorBasedMetaBufferSpec" should "add DelayMessageMeta correctly" in {
    val buffer = new VectorBasedTimerBucket(new AtomicLong(0), 6)
    (0 until 100).foreach { index =>
      buffer.add(new DelayMessageMeta(index, index + 1))
    }

    var index = 0
    buffer.foreach(e => {
      assert(e.offset == index)
      assert(e.expirationMsAbsolutely == e.offset + 1)
      index += 1
    })
  }

  "VectorBasedMetaBufferSpec" should "flush correctly" in {
    val buffer = new VectorBasedTimerBucket(new AtomicLong(0), 6)
    (0 until 100).foreach { index =>
      buffer.add(new DelayMessageMeta(index, index + 1))
    }

    buffer.flush( e => assert(e.offset + 1 == e.expirationMsAbsolutely))
    var count = 0
    buffer.foreach { _ =>
      count  += 1
    }
    assert(count == 0)
  }

  "VectorBasedMetaBufferSpec" should "have correctly delay" in {
    val buffer = new VectorBasedTimerBucket(new AtomicLong(0), 6)
    buffer.setExpiration(SystemTime.hiResClockMs + 10 * 1000)
    val delay = buffer.getDelay(TimeUnit.SECONDS)
    assert(delay == 10 || delay == 9)
  }

  "VectorBasedMetaBufferSpec" should "compare each other correctly" in {
    val middle = new VectorBasedTimerBucket(new AtomicLong(0), 6)
    val smaller = new VectorBasedTimerBucket(new AtomicLong(0), 6)
    val bigger = new VectorBasedTimerBucket(new AtomicLong(0), 6)
    middle.setExpiration(10)
    smaller.setExpiration(9)
    bigger.setExpiration(11)
    assert(middle.compareTo(middle) == 0)
    assert(middle.compareTo(bigger) == -1)
    assert(middle.compareTo(smaller) == 1)
  }
}
