package kafka.delay.message.client.cache

import org.roaringbitmap.RoaringBitmap

import scala.annotation.tailrec

/**
  * 用于存储offset的值，考虑到所占用的内存大小。
  * 主要被RecordCache用于存储已经永久移除的offset的值。
  */
class OffsetCache(maxSizeInByte: Int) {
  private[this] val bitmap = new RoaringBitmap()
  private[this] var base: Long = -1
  private var count = 0
  val checkInterval = 1024

  def contains(offset: Long) = {
    if (count == 0) {
      false
    } else {
      bitmap.contains((offset - base).toInt)
    }
  }

  @tailrec
  final def add(offset: Long): Unit = {
    if(count % checkInterval == 0) {
      maybeTrim()
    }
    if (count == 0) {
      init(offset)
    } else if (offset - base > Int.MaxValue) {
      reset()
      add(offset)
    } else {
      bitmap.add((offset - base).toInt)
      count += 1
    }
  }

  def size = count
  def sizeInByte = bitmap.getSizeInBytes

  @tailrec
  private def maybeTrim(): Unit = {
    if (bitmap.getSizeInBytes > maxSizeInByte) {
      bitmap.trim()
    }
    //try again
    if (bitmap.getSizeInBytes > maxSizeInByte) {
      //clear what
      val last = bitmap.last()
      val first = bitmap.first()
      bitmap.remove(first.toLong, (first + (last - first) / 2).toLong)
      maybeTrim()
    }
  }

  private def init(offset: Long): Unit = {
    count = 1
    base = offset
    bitmap.add(0)
  }

  def reset(): Unit = {
    bitmap.clear()
    base = -1
    count = 0
  }
}
