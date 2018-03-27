package kafka.delay.test.unit.timer.bucket

import java.util

import kafka.delay.message.timer.bucket.SortedCompressedLongArray
import org.scalatest.{FlatSpec, Matchers}

import scala.collection.SortedSet
import scala.util.Random

class SortedCompressedLongArraySpec extends FlatSpec with Matchers {
  "SortedCompressedLongArray" should "compress and decompress correctly" in {
    val toCompress = new Array[Long](10000)
    var i = 0
    while(i < toCompress.length) {
      toCompress(i) = Math.abs(Random.nextLong() % 1000000)
      i += 1
    }
    val sortedToCompress = SortedSet(toCompress: _*).toArray
    val min = sortedToCompress.min
    val compressed = SortedCompressedLongArray(sortedToCompress)
    val uncompressed = compressed.uncompact()
    val base = compressed.base
    println("compress ratio is " + sortedToCompress.length * 8 / compressed.sizeInByte)
    base shouldBe min
    uncompressed.length shouldBe sortedToCompress.length
    sortedToCompress.zipWithIndex.foreach {
      case(e, index) =>
        e shouldBe uncompressed(index)
    }
  }
}
