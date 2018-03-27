package kafka.delay.test.unit.timer.bucket

import kafka.delay.message.timer.bucket.MetaBuffer
import kafka.delay.message.timer.meta.DelayMessageMeta
import org.scalatest.{FlatSpec, Matchers}

class MetaBufferSpec extends FlatSpec with Matchers {
  "MetaVector" should "have correct size" in {
    val buffer = new MetaBuffer()
    assert(buffer.size == 0)
    val capacity = buffer.capacity
    (0 until 2 * capacity).foreach { index =>
      buffer += new DelayMessageMeta(0, 0)
    }
    assert(buffer.size == 2 * capacity)
  }

  "MetaVector" should "return correctly iterator after grow up" in {
    val buffer = new MetaBuffer()
    val capacity = buffer.capacity
    (0 until 2 * capacity).foreach { index =>
      buffer += new DelayMessageMeta(index, index + 1)
    }
    assert(buffer.size == 2 * capacity)
    buffer.iterator.zipWithIndex.foreach { e =>
      assert(e._1.offset == e._2 && e._2 == e._1.expirationMsAbsolutely - 1)
    }
  }

  "MetaVector" should "trim correctly" in {
    val buffer = new MetaBuffer(10)
    buffer += new DelayMessageMeta(0, 0)
    buffer.trim()
    assert(buffer.size == 1)
  }

  "MetaVector" should "reset correctly" in {
    val buffer = new MetaBuffer(10)
    (0 until 100).foreach{ _ =>
      buffer += new DelayMessageMeta(0, 0)
    }
    buffer.reset()
    assert(buffer.size == 0)
    assert(buffer.capacity < 100)
    val iter = buffer.iterator
    assert(iter.size == 0)
  }
}
