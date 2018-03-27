package kafka.delay.message.timer.bucket

import me.lemire.integercompression.differential.IntegratedIntCompressor

/**
  * 一个排序后的数组
  * @param base 基数，也是解压后的array of long里边的最小值
  * @param length 包含的long的个数
  * @param compressed
  */
class SortedCompressedLongArray(override val base: Long,
                                override val length: Int,
                                compressed: Array[Int])
  extends SortedCompactedLongArray {
  override def uncompact(): Array[Long] = {
    val iic = new IntegratedIntCompressor
    val uncompressedInts = iic.uncompress(compressed)
    val uncompressedLongs = new Array[Long](length)
    var i = 0
    while (i < length) {
      uncompressedLongs(i) = uncompressedInts(i) + base
      i += 1
    }
    uncompressedLongs
  }
  override val sizeInByte = compressed.length * 4
}

object SortedCompressedLongArray {
  /**
    *
    * @param data 必须非空 必须是排序后的
    */
  def apply(data: Array[Long]): SortedCompressedLongArray = {
    require(data.length > 0)
    val length = data.length
    val base = data(0)
    val sortedInt = new Array[Int](length)
    var i = 0
    while (i < length) {
      val diff = data(i) - base
      if (diff > Int.MaxValue) {
        throw new IllegalArgumentException(s"$diff with ${data(i)} is big than max int")
      }
      sortedInt(i) = diff.toInt
      i += 1
    }
    val iic = new IntegratedIntCompressor
    //这个方法要求作为参数的数据是排序好的
    val compressed = iic.compress(sortedInt)
    new SortedCompressedLongArray(base,  length, compressed)
  }
}
