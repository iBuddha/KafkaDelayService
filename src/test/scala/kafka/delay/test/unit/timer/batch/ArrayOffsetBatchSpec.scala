package kafka.delay.test.unit.timer.batch

import kafka.delay.message.timer.meta.ArrayOffsetBatch
import org.scalatest.{FlatSpec, Matchers}

class ArrayOffsetBatchSpec extends FlatSpec with Matchers {
  "ArrayOffsetBatch" should "contains" in {
    val batch = new ArrayOffsetBatch(Array(1, 2, 3, 4))
    batch.contains(1) shouldBe true
    batch.contains(0) shouldBe false
  }

  "ArrayOffsetBatch" should "from" in {
    var batch = new ArrayOffsetBatch(Array(1, 2, 3, 4, 5))
    batch.from(1).get shouldEqual batch
    batch.from(6) shouldBe None
    batch.from(2).get shouldEqual new ArrayOffsetBatch(Array(2, 3, 4, 5))

    batch = new ArrayOffsetBatch(Array(1, 3, 5, 8, 10))
    batch.from(1).get shouldEqual batch
    batch.from(11) shouldBe None
    batch.from(3).get shouldEqual new ArrayOffsetBatch(Array(3, 5, 8, 10))
    batch.from(10).get shouldEqual new ArrayOffsetBatch(Array(10))
    batch.from(6).get shouldEqual new ArrayOffsetBatch(Array(8, 10))
    batch.from(5).get shouldEqual new ArrayOffsetBatch(Array(5, 8, 10))

    batch = ArrayOffsetBatch.Empty
    batch.from(0) shouldBe None
  }
}
