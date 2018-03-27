package kafka.delay.test.unit.timer.batch

import java.util

import kafka.delay.message.timer.meta.{BitmapOffsetBatch, BitmapOffsetBatchBuilder}
import org.scalatest.{FlatSpec, Matchers}

class BitmapOffsetBatchSpec extends FlatSpec with Matchers {

  "BitmapOffsetBatch" should "from" in {
    var batch = new BitmapOffsetBatch(1, 5, 1, new util.BitSet())
    batch.from(1).get shouldEqual batch
    batch.from(6) shouldBe None
    batch.from(2).get.toSeq shouldEqual Seq(2, 3, 4, 5)

    val builder = new BitmapOffsetBatchBuilder(100, 1, 10)
    Seq(1L, 3, 5, 8, 10).foreach(builder.add)
    batch = builder.build()
    //    batch = new ArrayOffsetBatch(Array(1, 3, 5, 8, 10))
    batch.from(1).get shouldEqual batch
    batch.from(11) shouldBe None
    batch.from(3).get shouldEqual Seq(3L, 5, 8, 10)
    batch.from(3).get.size shouldBe 4
    batch.from(10).get.toSeq shouldEqual Seq(10)
    batch.from(10).get.size shouldBe 1
    batch.from(6).get.toSeq shouldEqual Seq(8, 10)
    batch.from(6).get.size shouldBe 2
    batch.from(5).get.toSeq shouldEqual Seq(5, 8, 10)

    batch = BitmapOffsetBatch.Empty
    batch.from(0) shouldBe None
  }
}
