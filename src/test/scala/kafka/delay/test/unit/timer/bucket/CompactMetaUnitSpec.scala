package kafka.delay.test.unit.timer.bucket

import kafka.delay.message.timer.bucket.CompactedMetaArray
import kafka.delay.message.timer.meta.DelayMessageMeta
import org.scalatest.{FlatSpec, Matchers}

class CompactMetaUnitSpec  extends FlatSpec with Matchers {
  "CompactMetaUnit" should "remove duplicated metas and sort metas" in {
    val meta1 = new DelayMessageMeta(0, 1)
    val meta2 = new DelayMessageMeta(1, 2)
    val meta3 = new DelayMessageMeta(2, 2)

    val metas = Array(meta2, meta3, meta1, meta1, meta3)

    val processed = CompactedMetaArray.sortAndRemoveDuplicated(metas)
    processed.length shouldBe 3
    processed(0) shouldBe meta1
    processed(1) shouldBe meta2
    processed(2) shouldBe meta3

  }

}
