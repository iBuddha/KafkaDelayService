package kafka.delay.test.unit.client.cache

import kafka.delay.message.client.cache.OffsetCache
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class OffsetCacheSpec  extends FlatSpec with Matchers {
  "OffsetCache" should "have correct count" in {
    val cache = new OffsetCache(1024)
    cache.size shouldBe 0
    cache.add(1)
    cache.size shouldBe 1
    cache.add(2)
    cache.size shouldBe 2

    (0 until 10000).foreach { _ =>
      cache.add(Random.nextLong())
    }
    cache.size should be < 5000
  }

  "OffsetCache" should "contain its elements" in {
    val cache = new OffsetCache(Int.MaxValue / 1024)
    (1L until 1000).foreach(cache.add)
    (1L until 1000).foreach{ i =>
      cache.contains(i) shouldBe true
    }
  }

 "OffsetCache" should "trim" in {
   val cache = new OffsetCache(1024)
   val checkInterval = cache.checkInterval

   (0 until checkInterval + 1).foreach(_ => cache.add(Random.nextLong()))
   cache.sizeInByte should be < 1024
 }

}
