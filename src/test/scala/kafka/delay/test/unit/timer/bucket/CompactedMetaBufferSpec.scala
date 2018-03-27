package kafka.delay.test.unit.timer.bucket

import kafka.delay.message.timer.bucket.CompactMetaBuffer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.test.unit.timer.bucket.CompactedMetaBufferSpec._
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class CompactedMetaBufferSpec extends FlatSpec with Matchers {
  "CompactedMetaVector" should "add metas correctly" in {
    val metaBuffer = new CompactMetaBuffer(16, 128 * 128)
    val mocked = mockMessageMetas(128 * 128 * 2 + 1024)
    val sizeInByteBeforeCompress = mocked.length * 8 * 2
    mocked.foreach(metaBuffer +=)
    metaBuffer.size shouldBe mocked.length

    val sizeOfVector = metaBuffer.sizeInByte
    println("compression ratio: " + sizeInByteBeforeCompress.toDouble / sizeOfVector)

    val uncompressed = metaBuffer.iterator
    val sorted = uncompressed.toArray.sorted
    sorted.length shouldBe metaBuffer.size
    var i = mocked.length
    while(i < mocked.length){
      sorted(i) shouldBe mocked(i)
      i += 1
    }

  }
}

object CompactedMetaBufferSpec {
  def mockMessageMetas(number: Int): Array[DelayMessageMeta] = {
    val base = 1000000L
    val metas = new Array[DelayMessageMeta](number)
    var i = 0
    while(i < number) {
      val offset = base + i
      val absExpireMs = System.currentTimeMillis() + i + Random.nextInt(1000000)
      metas(i) = new DelayMessageMeta(offset, absExpireMs)
      i += 1
    }
    metas
  }
}
