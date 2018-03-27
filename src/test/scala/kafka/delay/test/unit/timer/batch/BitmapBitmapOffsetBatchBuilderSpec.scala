package kafka.delay.test.unit.timer.batch

import kafka.delay.message.timer.meta.BitmapOffsetBatchBuilder
import org.scalatest.{FlatSpec, Matchers}

class BitmapBitmapOffsetBatchBuilderSpec extends FlatSpec with Matchers {

  "BitmapOffsetBatchBuilder" should "contains all inserted elements" in {
    //    val offsets = List(0L)
    val offsets = List(6L, 9, 10, 11, 13, 15, 16)
    val builder = new BitmapOffsetBatchBuilder(20, 5, 3)
    offsets.foreach(builder.add)
    val batch = builder.build()

    batch should (contain theSameElementsInOrderAs offsets)
  }

  "BitmapOffsetBatchBuilder" should "return false when first element exceeds max distance" in {
    val builder = new BitmapOffsetBatchBuilder(range = 10, min = 1, maxDistance = 2)
    builder.add(4) shouldBe false
  }

  "BitmapOffsetBatchBuilder" should "build a batch with max value correctly assigned" in {
    var builder = new BitmapOffsetBatchBuilder(range = 10, min = 1, maxDistance = 2)
    builder.add(1)
    builder.add(2)
    var batch = builder.build()
    batch.min shouldBe 1
    batch.max shouldBe 2

    builder = new BitmapOffsetBatchBuilder(range = 10, min = 1, maxDistance = 4)
    builder.add(1)
    batch = builder.build()
    batch.min shouldBe 1
    batch.max shouldBe 1

  }

  "An empty BitmapOffsetBatchBuilder" should "be returned when no element is added" in {
    val builder = new BitmapOffsetBatchBuilder(range = 10, min = 0, maxDistance = 3)
    builder.build() should be (empty)
  }

  "BitmapOffsetBatchBuilder" should "have correct index" in {
    val builder = new BitmapOffsetBatchBuilder(10, min = 11, 9)
    builder.add(12)
    builder.add(14)
    builder.add(17)
    val batch = builder.build()
    batch.length shouldBe 3
    batch(0) shouldBe 12L
    batch(1) shouldBe 14L
    batch(2) shouldBe 17L
    a [ArrayIndexOutOfBoundsException] should be thrownBy(batch(3))
  }

  "BitmapOffsetBatchBuilder" should "refuse to insert element exceed max distance" in {
    val builder = new BitmapOffsetBatchBuilder(range = 10, 0, 3)
    builder.add(0L)
    builder.add(4L) shouldBe false
    builder.add(5L) shouldBe false
  }

  "BitmapOffsetBatchBuilder" should "be full when batch is fully populated" in {
    val offsets = (0L until 10000)
    val builder = new BitmapOffsetBatchBuilder(range= 100000, 0, maxDistance = 2)
    offsets.foreach(builder.add)
    val batch = builder.build()
    batch.isFull shouldBe true
  }

  "BitmapOffsetBatchBuilder" should "be not full when batch is not fully populated" in {
    val offsets = List(0L, 1L, 3L)
    val builder = new BitmapOffsetBatchBuilder(range= 1000, 0, maxDistance = 2)
    offsets.foreach(builder.add)
    val batch = builder.build()
    batch.isFull shouldBe false
  }
}
