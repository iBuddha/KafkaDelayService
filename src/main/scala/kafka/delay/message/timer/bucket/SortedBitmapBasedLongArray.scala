package kafka.delay.message.timer.bucket

import org.roaringbitmap.{IntConsumer, RoaringBitmap}

/**
  *
  * @param base
  * @param length
  * @param bitmap
  */
class SortedBitmapBasedLongArray(override val base: Long,
                                 override val length: Int,
                                 bitmap: RoaringBitmap)
  extends SortedCompactedLongArray {
  override def uncompact() = {
    val uncompacted = new Array[Long](length)
    var i = 0
    val intConsumer = new IntConsumer {
      override def accept(value: Int): Unit = {
        uncompacted(i) = value + base
        i += 1
      }
    }
    bitmap.forEach(intConsumer)
    uncompacted
  }

  override def sizeInByte = bitmap.getSizeInBytes
}

object SortedBitmapBasedLongArray {
  /**
    * data必须是非重复的
    * @param data
    * @return
    */
  def apply(data: Array[Long]): SortedBitmapBasedLongArray = {
    require(data.length > 0)
    val bitmap = new RoaringBitmap()
    val length = data.length
    val base = data(0)
    var i = 0
    while( i < length) {
      val diff = data(i) - base
      if (diff > Int.MaxValue) {
        throw new IllegalArgumentException(s"$diff with ${data(i)} is big than max int")
      }
      bitmap.add(diff.toInt)
      i += 1
    }
    new SortedBitmapBasedLongArray(base, length, bitmap)
  }
}
