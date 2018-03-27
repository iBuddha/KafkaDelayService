package kafka.delay.test.unit.timer.batch

import kafka.delay.message.timer.meta.ArrayOffsetBatchBuilder
import org.scalatest.{FlatSpec, Matchers}

class ArrayOffsetBatchBuilderSpec extends FlatSpec with Matchers {
  "ArrayOffsetBatchBuilder" should "build" in {
    var builder = new ArrayOffsetBatchBuilder(100, 0, 1)
    builder.build().size shouldBe 0

    builder = new ArrayOffsetBatchBuilder(100, 0, 1)
    (0L until 10).foreach(builder.add)
    builder.add(12) shouldBe false
    builder.build().iterator.toSeq shouldEqual Seq((0 until 10): _*)
  }
}
