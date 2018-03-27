package kafka.delay.message.utils

import java.sql.Timestamp

import kafka.delay.message.timer.meta.DelayMessageMeta

import scala.collection.SortedSet

/**
  * Created by xhuang on 18/07/2017.
  */
object Utils {

  @inline
  def milliToString(millis: Long): String = new Timestamp(millis).toString

  @inline
  def sortedAndRemoveDuplicated(metas: Array[DelayMessageMeta]): Array[DelayMessageMeta] = {
    val sorted = SortedSet[DelayMessageMeta]() ++ metas
    sorted.toArray
  }
}
