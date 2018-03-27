package kafka.delay.message.timer.bucket

/**
  * 一个压缩后的long型数组。需要解压以获取压缩前的值
  */
sealed trait CompactedLongArray {

  /**
    * 压缩前的数组里的最小值。
    * timer可以依据base可以在不解压数组的情况下进行判断。比如是否这个数组是否可能有元素expire
    * TODO: 据此对timer进行优化
    * @return
    */
  def base: Long

  def uncompact(): Array[Long]

  def length: Int

  def sizeInByte: Int
}

/**
  * 压缩前的数据是排序好的并且去重以后的。这样可以利用某些算法进行优化。
  * 比如，在实现中用到的me.lemire.integercompression.differential.IntegratedIntCompressor
  * 以及在去重后，可以使用RoaringBitmap
  */
trait SortedCompactedLongArray extends CompactedLongArray

/**
  * 在压缩前的数组是没有排序的，在解压后需要保留原始顺序。
  */
trait UnsortedCompactedLongArray extends CompactedLongArray

