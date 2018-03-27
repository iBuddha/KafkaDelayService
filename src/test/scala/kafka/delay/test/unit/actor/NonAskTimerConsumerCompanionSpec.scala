package kafka.delay.test.unit.actor

import org.scalatest.{FlatSpecLike, Matchers}

import scala.collection.mutable
import kafka.delay.message.actor.NonAskTimerConsumer
import kafka.delay.message.timer.meta.{BitmapOffsetBatch, BitmapOffsetBatchBuilder, OffsetBatch}

class NonAskTimerConsumerCompanionSpec extends FlatSpecLike with Matchers{
  "NonAskTimerConsumer" should "drain batches with total size not bigger than max allowed size" in {
    val stashedBatches =  new mutable.ArrayBuffer[OffsetBatch]
    (1 to 10).foreach { size =>
      stashedBatches +=  NonAskTimerConsumerCompanionSpec.newMetaBatch(size)
    }
    val batchList = NonAskTimerConsumer.drainBatchList(stashedBatches, 10)
    batchList.size shouldBe 4
    batchList.map(_.size).toArray shouldBe Array(1, 2, 3, 4)
  }

}

object NonAskTimerConsumerCompanionSpec {
  def newMetaBatch(size: Int): BitmapOffsetBatch = {
    val builder = new BitmapOffsetBatchBuilder(size, 0, 1)
    (0 until size).foreach { i =>
      builder.add(i)
    }
    builder.build()
  }
}
