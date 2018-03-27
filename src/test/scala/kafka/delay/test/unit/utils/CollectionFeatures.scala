package kafka.delay.test.unit.utils

import kafka.delay.message.timer.meta.DelayMessageMeta
import org.scalatest.{FlatSpecLike, Matchers}

import scala.collection.immutable.SortedSet

class CollectionFeatures extends FlatSpecLike with Matchers {
  "sorted and remove duplicated" should "work" in {
    val offsets = Seq(0L, 0L, 4L, 4L, 2L, 2L, 11L, 13L)
    val metas = offsets.map(new DelayMessageMeta(_, 0L))
    val sorted = SortedSet[DelayMessageMeta]() ++ metas
    val a = sorted.toArray.map(_.offset)
    a.size shouldBe 5
    a shouldEqual Array(0L, 2L, 4L, 11L, 13L)
  }
}
