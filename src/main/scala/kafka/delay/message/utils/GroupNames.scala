package kafka.delay.message.utils

object GroupNames {
  /**
    * 消费delay topic，从用户消息获取元数据
    * @param baseTopic
    * @return
    */
  def metaConsumerGroup(baseTopic: String) = s"delay-service-meta-$baseTopic"

  /**
    * 从kafka里获取之前记录的元数据
    * @param baseTopic
    * @return
    */
  def storageConsumerGroup(baseTopic: String) = s"delay-service-storage-$baseTopic"

  /**
    * 从Kafka里查找已超时的消息
    * @param baseTopic
    * @return
    */
  def messageConsumerGroup(baseTopic: String) = s"delay-service-message-$baseTopic"

  def cacheWarmerConsumerGroup(baseTopic: String) = s"delay-service-cache-warmer-$baseTopic"
}
