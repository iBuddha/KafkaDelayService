package kafka.delay.message.client.cache


import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}

/**
  * 用于估算单条record的平均大小。
  * 用于在批量拉取消息之前估算出拉取后消息的可能大小，以避免一次拉取的消息占用太大的内存。
  *
  * a simple EWMA implementation
  */
class RecordSizeSampler {
  private[this] val currentFactor = 0.05
  private[this] val beforeFactor = 0.95
  private var current = 1024 * 1024.0 //should be max record size of a kafka cluster

  def average: Double = current

  def  +=(record: ConsumerRecord[Array[Byte], Array[Byte]]): Unit = {
    val recordSize = RecordSizeSampler.bytes(record)
    current = recordSize * currentFactor + current * beforeFactor
  }

//  def ++=(records: ConsumerRecords[])

}

object RecordSizeSampler {

  /**
    * 固定部分的大小
    * 开启指针压缩
    *
    * val NO_TIMESTAMP: Long = Record.NO_TIMESTAMP
    * val NULL_SIZE: Int = -(1)
    * val NULL_CHECKSUM: Int = -(1)
    * *
    * private val topic: String = null
    * private val partition: Int = 0
    * private val offset: Long = 0L
    * private val timestamp: Long = 0L
    * private val timestampType: TimestampType = null
    * private val checksum: Long = 0L
    * private val serializedKeySize: Int = 0
    * private val serializedValueSize: Int = 0
    * private val key: K = null
    * private val value: V = null
    */
  val FixedSize: Int =
    16 + //header
      4 + //topic
      4 + //partition
      8 + //offset
      8 + //timestamp
      4 + // timestamp type，这是一个enum
      8 + //checksum
      4 + //key size
      4 + //value size
      8 + 12 + // key array overhead
      8 + 12 //value array overhead


  def bytes(record: ConsumerRecord[Array[Byte], Array[Byte]]): Int = {
    FixedSize + record.key().length + record.value().length
  }
}
