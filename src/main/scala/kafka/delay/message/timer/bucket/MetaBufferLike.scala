package kafka.delay.message.timer.bucket

import kafka.delay.message.timer.meta.DelayMessageMeta

/**
  * append only list of DelayMessageMeta
  */
trait MetaBufferLike extends Iterable[DelayMessageMeta] {
  def +=(meta: DelayMessageMeta): Unit
  def size: Int
}
