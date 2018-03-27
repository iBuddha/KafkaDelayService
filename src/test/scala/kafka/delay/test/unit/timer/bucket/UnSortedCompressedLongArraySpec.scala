package kafka.delay.test.unit.timer.bucket

import kafka.delay.message.timer.bucket.{UnSortedCompressedLongArray, SortedCompressedLongArray}
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class UnSortedCompressedLongArraySpec extends FlatSpec with Matchers {
  "UnSortedCompressedLongArray" should "compress and decompress correctly" in {
    val toCompress = new Array[Long](10000)
    var i = 0
    while(i < toCompress.length) {
      toCompress(i) = Random.nextLong() % 1000000 + 19
      i += 1
    }
    val min = toCompress.min
    val unit = UnSortedCompressedLongArray(toCompress)
    val uncompressed = unit.uncompact()
    val base = unit.base
    base shouldBe min
    uncompressed.length shouldBe toCompress.length
    toCompress.zipWithIndex.foreach {
      case(e, index) =>
        e shouldBe uncompressed(index)
    }
  }
}
