package kafka.delay.message.timer.bucket

import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.{Delayed, TimeUnit}

import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.SystemTime

import scala.math.max

/**
  * 它就是timing wheel里的一个bucket
  */
class VectorBasedTimerBucket(messageCounter: AtomicLong, initSize: Int = 64) extends TimerBucket {

  private val metaVector = new CompactMetaBuffer(16)
  private[this] val expiration = new AtomicLong(-1L)

  def size() = metaVector.size

  override def setExpiration(expirationMs: Long): Boolean = expiration.getAndSet(expirationMs) != expirationMs

  override def getExpiration(): Long = expiration.get()

  override def add(delayMeta: DelayMessageMeta): Unit = synchronized {
    metaVector += delayMeta
    messageCounter.incrementAndGet()
  }

  override def foreach(f: (DelayMessageMeta) => Unit): Unit = synchronized {
    metaVector.iterator.foreach(f)
  }

  override def flush(f: (DelayMessageMeta) => Unit): Unit = synchronized {
    val flushedNum = metaVector.size
    foreach(f)
    metaVector.reset()
    var cleaned = false
    while (!cleaned) {
      val currentNum = messageCounter.get()
      cleaned = messageCounter.compareAndSet(currentNum, currentNum - flushedNum)
    }
  }

  override def getDelay(unit: TimeUnit): Long =
    unit.convert(max(getExpiration - SystemTime.hiResClockMs, 0), TimeUnit.MILLISECONDS)

  override def compareTo(d: Delayed): Int = {
    val other = d.asInstanceOf[TimerBucket]
    if (getExpiration < other.getExpiration) -1
    else if (getExpiration > other.getExpiration) 1
    else 0
  }
}
