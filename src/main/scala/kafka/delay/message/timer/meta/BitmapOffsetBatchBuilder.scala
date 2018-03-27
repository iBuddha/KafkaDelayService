package kafka.delay.message.timer.meta

import java.util

import org.slf4j.LoggerFactory

/**
  * 用于构造一个MetaBatch。提供给它的offset必须是从小到大排列好的。
  * 而且需要注意add返回的boolean值，如果不能添加成功，就需要另建一个新的
  *
  * @param range       (max offset in batch  - min offset in batch) + 1, 并不是指实际元素的最大数量
  *                    这个参数决定了consumer在拉取这个batch里的消息时需要读取的消息的条数
  * @param min         offset最小的消息的offset, 值为min的offset必须在batch中
  * @param maxDistance 相邻两个消息的offset所允许的最大差值。比如，当设为2时，1,3是允许的，当是1，4时，就不能加入
  */
class BitmapOffsetBatchBuilder(range: Long, min: Long, maxDistance: Long) extends OffsetBatchBuilder {
  require(range >= 1, "range should >= 1")
  require(maxDistance > 0, "max distance should > 0")
  val bitset: util.BitSet = new util.BitSet(0)
  var pre: Long = min - 1
  val max: Long = min + range - 1

  /**
    * 被添加的offset应该是从小到大排列好的
    *
    * @param offset offset
    * @return true表示成功加入当前batch, false表示不能加入，此时需要建立一个新的batch, 然后再把这个offset加到新的batch里
    */
  override def add(offset: Long): Boolean = {
    if (offset < pre)
      throw new IllegalStateException(s"please sort before add is called, pre is $pre, current is $offset")
    //    if (offset == pre)
    //      MetaBatchBuilder.logger.
    if (offset > max) {
      false
    } else {
      val distance = offset - pre
      if (distance > maxDistance) {
        false
      } else {
        //qualified to add
        if (distance != 1) {
          var missed = pre + 1
          while (missed < offset) {
            bitset.set((missed - min).toInt)
            missed = missed + 1
          }
        }
        pre = offset
        true
      }
    }
  }

  override def build(): BitmapOffsetBatch = {
    new BitmapOffsetBatch(min, pre, base = min, bitset)
  }
}

object BitmapOffsetBatchBuilder {
  val logger = LoggerFactory.getLogger(BitmapOffsetBatchBuilder.getClass)
}