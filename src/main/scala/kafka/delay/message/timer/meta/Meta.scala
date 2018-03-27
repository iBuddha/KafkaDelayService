package kafka.delay.message.timer.meta

import java.nio.ByteBuffer

import kafka.delay.message.utils.Time

/**
  *
  * @param offset 这个消息在delay topic中的offset
//  * @param expirationMs 这个消息的超时时间，这是一个clock time
  * @param expirationMsAbsolutely 这个消息的超时时间，这是一个绝对时间，是使用System.nanoTime计算出来的
  *                               计算方法为：读取这个消息的时候，获取它的expirationMs, 然后依据当前的System.currentMilliseconds计
  *                               算出来需要延迟的时间，即delayMs。把delayMs + System.nanoTime，得到绝对的超时时间。
  */
case class DelayMessageMeta(val offset: Long, val expirationMsAbsolutely: Long) extends Ordered[DelayMessageMeta] {
  override def toString(): String = {
    s"offset: $offset，delayMs: $expirationMsAbsolutely"
  }

  override def compare(that: DelayMessageMeta) = {
    if(this.offset < that.offset) -1 else if(this.offset == that.offset) 0 else 1
  }

}

object DelayMessageMeta {

  /**
    * 只有当一个DelayMessageMeta第一次被读取时，才可以使用这个方法。以后在系统里的延迟时间，以expirationMsAbsolutely为准
    * @return
    */
  def fromClockTime(offset: Long, expirationMs: Long, time: Time): DelayMessageMeta = {
    val expirationMsAbs = expirationMs - time.milliseconds + time.hiResClockMs
    new DelayMessageMeta(offset, expirationMsAbs)
  }

  def fromClockTime(offset: Long,
                    expireMs: Long,
                    currentClockTime: Long,
                    currentAbsTime: Long): DelayMessageMeta = {
    val expireMsAbs = expireMs - currentClockTime + currentAbsTime
    new DelayMessageMeta(offset, expireMsAbs)
  }

  def readFrom(buffer: ByteBuffer): DelayMessageMeta = {
    val offset = buffer.getLong
//    val expireMs = buffer.getLong
    val absExpireMs = buffer.getLong
    new DelayMessageMeta(offset, absExpireMs)
  }

  def writeTo(meta: DelayMessageMeta, buffer: ByteBuffer): Unit = {
//    buffer.putLong(meta.expirationMs)
    buffer.putLong(meta.offset)
    buffer.putLong(meta.expirationMsAbsolutely)
  }

  //  val expireTimeSize = java.lang.Long.BYTES
  private val offsetSize = java.lang.Long.BYTES
  private val absExpireTimSize = java.lang.Long.BYTES
  val SizeInByte =  offsetSize + absExpireTimSize
}

