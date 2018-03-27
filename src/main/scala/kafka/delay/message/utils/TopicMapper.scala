package kafka.delay.message.utils

/**
  * 用于决定Delay Service对于每个topic所创建的相关topic的命名规则
  * 假如base topic叫 __apple__
  * 那么：
  * 接收到 delaying message的topic叫: __apple-delay__
  * 收到expired message的消息为: __apple-expired__
  * 用于记录已发送完毕的消息的元数据的topic为: __apple-expire-meta__
  */
object TopicMapper {

  val delayTopicSuffix = "-delay"
  val expiredTopicSuffix = "-expired"
  val metaTopicSuffix = "-delay-meta"

  /**
    * delay topic用于存储用户发送的需要进行delay的消息
    */
  def getDelayTopicName(baseTopic: String) = baseTopic + delayTopicSuffix

  /**
    * expired topic用于存储已经delay结束的消息
    */
  def getExpiredTopicName(baseTopic: String) = baseTopic + expiredTopicSuffix

  /**
    * 用于存储被delay消息的元数据，以及消除已完成的delay消息的元数据。这需要是一个compact topic
    */
  def getMetaTopicName(baseTopic: String) = baseTopic + metaTopicSuffix

  /**
    * @param topicName 如果满足上述三个topic的命名条件，就返回Some，否则为None
    * @return
    */
  def getBaseName(topicName: String): Option[String] = {
    maybeDrop(topicName, delayTopicSuffix)
      .orElse(maybeDrop(topicName, expiredTopicSuffix))
      .orElse(maybeDrop(topicName, metaTopicSuffix))
  }

  private def maybeDrop(from: String, suffix: String): Option[String] = {
    if (from.endsWith(suffix))
      Some(from.dropRight(suffix.length))
    else
      None
  }
}
