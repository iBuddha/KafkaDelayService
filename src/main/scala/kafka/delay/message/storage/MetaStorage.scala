package kafka.delay.message.storage

import java.io.IOException

import kafka.delay.message.timer.MessageTimer

/**
  * 用于记录一个消息跟delay相关的元数据到一个持久化的存储.
  * 跟delay相关的元数据为，假设消息为M
  * # M的topic
  * # M的partition
  * # M的offset
  * # M的expire time (in clock time)
  *
  */
trait MetaMarker {

  /**
    * 保存一个需要被delay的消息的元数据
    */

  @throws[IOException]
  def store(meta: StoreMeta): Unit

  @throws[IOException]
  def store(metas: Seq[StoreMeta]): Unit
}

trait MetaRemover {
  /**
    * 标记一条消息为已经delay完毕，发送给了expired topic
    * @return 表示标记结果的future。
    */
  @throws[IOException]
  def delete(meta: StoreMeta): Unit

  @throws[IOException]
  def delete(metas: Seq[StoreMeta]): Unit
}

trait MetaStorage extends MetaMarker with MetaRemover {
  /**
    * 恢复一个topic的所有timer到正确状态。使得TopicDelayService的MessageConsumer可以从被返回的位置继续处理。
    *
    * @param baseTopic
    * @param utilDelayTopicOffset 只处理delay topic中offset在此之前的消息。通常这是meta consumer上次停止的位置
    * @param timers
    * @param ignoredExpireBeforeMs 不处理超时时间小于此的消息
    * @return
    */
  @throws[IOException]
  def restore(baseTopic: String,
              utilDelayTopicOffset: Map[Int, Long],
              timers: Seq[MessageTimer],
              ignoredExpireBeforeMs: Long): Unit

  def close()
}

/**
  *
  * @param baseTopic
  * @param partition 元数据所对应的被延迟的消息在 delay topic 里所在的partition
  * @param offset 元数据所对应的被延迟的消息在delay topic里的offset
  * @param expireMs 元数据所对应的被延迟的消息的超时时间, 以unix epoch记
  */
case class StoreMeta(baseTopic: String, partition: Int, offset: Long, expireMs: java.lang.Long)