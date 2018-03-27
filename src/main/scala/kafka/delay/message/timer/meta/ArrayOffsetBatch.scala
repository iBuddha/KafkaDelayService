package kafka.delay.message.timer.meta

import java.util

class ArrayOffsetBatch(private val offsets: Array[Long]) extends  OffsetBatch {
  override def length = offsets.length

  override def apply(idx: Int) = offsets(idx)

  override def iterator = offsets.iterator

  override def contains(offset: Long) = {
    util.Arrays.binarySearch(offsets, offset) >= 0
  }

  /**
    * 截取大于或等于参数的其余部分
    *
    * @param smallest inclusive
    * @return
    */
  override def from(smallest: Long): Option[ArrayOffsetBatch] = {
    if(offsets.length == 0){
      None
    } else if(smallest <= offsets.head) {
      Some(this)
    } else if(smallest > offsets.last) {
      None
    } else {
        var idx = 0 //idx of first element >= fromOffset
        while(smallest > offsets(idx)) {
          idx += 1
        }
      val result = new Array[Long](offsets.length - idx)
       Array.copy(offsets, idx, result, 0, result.length)
      Some(new ArrayOffsetBatch(result))
    }
  }

  override def equals(that: Any): Boolean = {
    that match{
      case batch: ArrayOffsetBatch =>
        if(util.Arrays.equals(batch.offsets, offsets)) true else false
      case _ => false
    }
  }
}

object ArrayOffsetBatch {
  val Empty = new ArrayOffsetBatch(new Array[Long](0))
}
