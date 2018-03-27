package kafka.delay.message.storage

import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.delay.message.storage.AbstractKafkaMetaStorage._
import kafka.delay.message.utils.TopicMapper._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.LongSerializer
/**
  * 使用一个compact topic来保存哪些消息待处理，哪些消息已处理
  *
  * 前置条件：
  * 1. MessageConsumer的last committed offset以后的消息都没有被处理，即从未被曾经加到过timer
  * 2. MessageConsumer的last committed offset之前的所有消息的metadata都已被发送给compact topic
  * 3. 所有已被处理过的消息都已在这个topic里被标记为可删除(用null)
  *
  * 因此，这个Storage在启动时，会消费compact topic里的所有消息，把所有未被消费的消息的metadata加到timer。
  *
  * @param bootstrapServers
  */
abstract class AbstractKafkaMetaStorage(bootstrapServers: String, metaTopic: String) extends MetaStorage {
  protected val producer = getProducer

  override def delete(meta: StoreMeta): Unit = sendAndGet(meta, delete = true)

  override def delete(metas: Seq[StoreMeta]): Unit = batchSendAndGet(metas, delete = true)

  override def store(meta: StoreMeta): Unit = sendAndGet(meta, delete = false)

  override def store(metas: Seq[StoreMeta]): Unit = batchSendAndGet(metas, delete = false)

  override def close() = producer.close()

  private def getProducer: KafkaProducer[MetaKey, java.lang.Long] = {
    val config = new Properties
    config.put(ProducerConfig.ACKS_CONFIG, "all")
    config.put(ProducerConfig.CLIENT_ID_CONFIG, "delay-service")
    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[MetaKeySerializer].getCanonicalName)
    config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[LongSerializer].getCanonicalName)
    new KafkaProducer[MetaKey, MetaValue](config)
  }


  private def send(meta: StoreMeta, value: java.lang.Long) = {
    producer.send(
      new ProducerRecord(metaTopic,
        meta.partition,
        MetaKey(meta.partition, meta.offset),
        value)
    )
  }

  private def sendAndGet(meta: StoreMeta, delete: Boolean): Unit = {
    if (delete)
      send(meta, null).get(sendTimeout, sendTimeUnit)
    else
      send(meta, meta.expireMs).get(sendTimeout, sendTimeUnit)
  }

  private def batchSendAndGet(metas: Seq[StoreMeta], delete: Boolean): Unit = {
    val futures = if (delete)
      metas.map(m => send(m, null))
    else
      metas.map(m => send(m, m.expireMs))

    producer.flush()
    futures.foreach(_.get)
  }
}

object AbstractKafkaMetaStorage {
  val sendTimeout = 10
  val sendTimeUnit = TimeUnit.MINUTES
  if (java.lang.Long.SIZE != 64) {
    throw new Error("long.size is asserted to be 64")
  }

  case class MetaKey(partition: Int, offset: Long)

  type MetaValue = java.lang.Long
}
