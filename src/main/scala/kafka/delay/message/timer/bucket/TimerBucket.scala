package kafka.delay.message.timer.bucket

import java.util.concurrent.Delayed

import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.utils.threadsafe

/**
  * Created by xhuang on 18/07/2017.
  * 用于#在内存中#存储被延迟发送的消息的元数据
  *
  */
@threadsafe
trait TimerBucket extends Delayed {
  /**
    * 如果以前的expireMs跟将要设置的相等，就返回false, 如果不等，就返回true
    */
  def setExpiration(expirationMs: Long): Boolean
  def getExpiration(): Long
  def add(delayMeta: DelayMessageMeta)
  def foreach(f: (DelayMessageMeta) => Unit): Unit
  def flush(f: (DelayMessageMeta) => Unit): Unit
  def size(): Int
}