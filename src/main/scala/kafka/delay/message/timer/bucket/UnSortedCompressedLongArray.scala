package kafka.delay.message.timer.bucket

import java.util

import me.lemire.integercompression.{Composition, FastPFOR, IntWrapper, VariableByte}

/**
  * 保留压缩前的数据
  *
  * @param base
  * @param length
  * @param compressed
  */
class UnSortedCompressedLongArray(override val base: Long,
                                  override val length: Int,
                                  compressed: Array[Int])
  extends UnsortedCompactedLongArray {
  override def uncompact(): Array[Long] = {
    val codec = new Composition(new FastPFOR, new VariableByte)
    val recovered = new Array[Int](length)
    val recOffset = new IntWrapper(0)
    codec.uncompress(compressed, new IntWrapper(0), compressed.length, recovered, recOffset)
    val longArray = new Array[Long](length)
    var i = 0
    while (i < length) {
      longArray(i) = recovered(i) + base
      i += 1
    }
    longArray
  }

  override val sizeInByte = compressed.length * 4
}

object UnSortedCompressedLongArray {
  /**
    *
    * @param data 必须非空
    *             不需要是排序过的，会在方法内进行排序
    *             需要注意的是，解压后的数组是排序后的，而且其顺序不一定等于压缩前的顺序
    */
  def apply(data: Array[Long]): UnSortedCompressedLongArray = {
    require(data.length > 0)
    val length = data.length
    val smallest = min(data)
    val intArray = new Array[Int](length)
    var i = 0
    while (i < length) {
      val diff = data(i) - smallest
      if (diff > Int.MaxValue) {
        throw new IllegalArgumentException(s"$diff with ${data(i)} is big than max int")
      }
      intArray(i) = diff.toInt
      i += 1
    }
    val codec = new Composition(new FastPFOR, new VariableByte)
    var supplement = 1024
    val inputOffset = new IntWrapper(0)
    val outputOffset = new IntWrapper(0)
    var compressed: Array[Int] = null
    var success = false
    while (!success) {
      try {
        compressed = new Array[Int](length + supplement)
        codec.compress(intArray, inputOffset, length, compressed, outputOffset)
        success = true
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          success = false
          supplement += supplement
      }
    }
    compressed = util.Arrays.copyOf(compressed, outputOffset.intValue)
    new UnSortedCompressedLongArray(smallest, length, compressed)
  }

  def min(data: Array[Long]): Long = {
    var min = data(0)
    val length = data.length
    var i = 1
    while (i < length) {
      val current = data(i)
      if (current < min) {
        min = current
      }
      i += 1
    }
    min
  }
}