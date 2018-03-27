package kafka.delay.message.timer.meta

import scala.collection.mutable.ArrayBuffer

class ArrayOffsetBatchBuilder(range: Long, min: Long, maxDistance: Long) extends OffsetBatchBuilder {
  require(range >= 1, "range should >= 1")
  require(maxDistance > 0, "max distance should > 0")
  private var buffer = new ArrayBuffer[Long]
  private var pre = min

  override def add(offset: Long): Boolean = {
    if (offset - pre > maxDistance ||  offset - min > range) {
      false
    } else {
      buffer += offset
      pre = offset
      true
    }
  }

  override def build() = new ArrayOffsetBatch(buffer.toArray)
}
