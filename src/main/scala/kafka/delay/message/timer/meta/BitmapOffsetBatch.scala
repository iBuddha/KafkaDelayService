package kafka.delay.message.timer.meta

import java.util

/**
  *
  * 但是Kafka consumer的拉取默认就是批量的，这样对于broker和client都是更有效率的方式。
  * 这个accumulator提供给Consumer的是一个MetaBatch，其中的offset大部分是相邻，适于consumer不断地拉取消息，
  * 并用这个MetaBatch决定拉取的起始点来终点，以及在拉取出来的ConsumerRecords里决定哪些record是需要的。
  * 可以使用 [[contains]]方法来判断一个offset在不在这个集合
  *
  * @param min         include min和max必须都在batch当中
  * @param max         include
  */
case class BitmapOffsetBatch(min: Long,
                             max: Long,
                             base: Long,
                             exclude: util.BitSet)
  extends OffsetBatch {
  //  override val length = (max - min + 1 - exclude.stream().count()).toInt
  //这个batch里的消息的实际数量
  override val size: Int = (max - min + 1 - exclude.cardinality()).toInt
  override val isEmpty: Boolean = min == max + 1

  override def length = size

  def contains(offset: Long): Boolean =
    if (offset > max || offset < min) false else !exclude.get((offset - base).toInt)

  override def foreach[U](f: (Long) => U): Unit =
    (min to max).filter(e => !exclude.get((e - base).toInt)).foreach(f)

  /**
    * sorted
    * @return
    */
  override def iterator: Iterator[Long] = (min to max).filter(contains).iterator

  def isFull = exclude.isEmpty

  override def apply(idx: Int): Long = {
    if (idx < 0 || idx >= length)
      throw new ArrayIndexOutOfBoundsException(idx)
    var position = -1
    var currentIdx = -1
    while (currentIdx < idx) {
      position = exclude.nextClearBit(position + 1)
      currentIdx += 1
    }
    min + position
  }

  override def toString(): String = {
    s"MetaBatch(min: $min, max: $max, base: $base)"
  }

  /**
    * 截取大于或等于参数的其余部分
    *
    * @param fromOffset inclusive
    * @return
    */
  override def from(fromOffset: Long): Option[BitmapOffsetBatch] = {
    if(this.eq(BitmapOffsetBatch.Empty) || fromOffset > max){
      None
    } else if(fromOffset <= min) {
      Some(this)
    } else {
      (fromOffset to max).find(contains).map { newMin =>
        val newBitSet = exclude.clone().asInstanceOf[util.BitSet]
        newBitSet.clear(0, (newMin - base).toInt)
        this.copy(min = newMin, exclude = newBitSet)
      }
    }
  }
}

object BitmapOffsetBatch {
  val Empty = BitmapOffsetBatch(-1, -2, -1, new util.BitSet())
}
