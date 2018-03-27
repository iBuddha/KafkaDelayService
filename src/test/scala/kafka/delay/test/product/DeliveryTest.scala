package kafka.delay.test.product

import java.util
import java.util.Properties
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{BlockingQueue, CountDownLatch, LinkedBlockingQueue}

import kafka.delay.message.utils.{Time, TopicMapper, Utils}
import kafka.delay.test.unit.kafka.producer.MockLongKeyProducer
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord, KafkaConsumer}
import org.apache.kafka.common.serialization.{LongDeserializer, StringDeserializer}

import scala.collection.mutable
import scala.util.Random

/**
  * 用于检查发送消息的数量和内容是否与收到的一致
  */
object DeliveryTest extends App {
  case class SendMessage(expireMs: Long, id: Int)
  case class ReceivedMessage(expireMs: Long, messageTs: Long, id: Int)

  private val bootstrapServers = "slave3.test:9092,slave2.test:9092"
  private val baseTopic = "foo"
  private val maxDelay = Time.MsPerSec * 60 * 120 //用于指定延迟的范围
//  private val maxDelay = Time.MsPerSec *  Time.SecsPerMin * 1 //用于指定延迟的范围
  private val tps = 200
  private val messageNumber = maxDelay / Time.MsPerSec * tps //要发送的消息数量

  private val delayTopic = TopicMapper.getDelayTopicName(baseTopic)
  private val delayTopicPartitionNumber = 2

  private val producer = new MockLongKeyProducer(delayTopic, bootstrapServers)

//  private def nextExpireMs = System.currentTimeMillis() + 5000
  private def nextExpireMs = Random.nextInt(maxDelay).toLong + System.currentTimeMillis() + 15000

  private val paddingSize = 100
  private val padding = ":" + "a" * paddingSize

  private var sendCount = 0
  private var messageId = 0

  val latch =
    new CountDownLatch(1)
  val outputQueue = new LinkedBlockingQueue[ConsumerRecord[Long, String]](Int.MaxValue)
  val collector = new MessageCollector(baseTopic, delayTopicPartitionNumber, bootstrapServers, outputQueue, latch)
  val collectorThread = new Thread(collector)
  collectorThread.start()
  latch.await()
  println("sending")
  val sent = mutable.Set.empty[SendMessage]
  val received = mutable.MutableList.empty[ReceivedMessage]
  try {
    (0 until messageNumber).grouped(100).foreach { group =>
      var batch = List.empty[(Long, String)]
      group.foreach { _ =>
        val expireMs = nextExpireMs
        val message = (expireMs, expireMs.toString + ":" + messageId + padding)
        assert(!sent.contains(SendMessage(expireMs, messageId)))
        sent += SendMessage(expireMs, messageId)
        batch = batch :+ message
        messageId = messageId + 1
      }
      producer.send(batch)
      sendCount += batch.size
      println(s"current sent count $sendCount")
    }
    println("all sent")
  } catch {
    case e: Exception => e.printStackTrace()
  } finally {
    producer.close()
  }

  Thread.sleep(maxDelay + 30000)
  collector.shutdown()
  collectReceived(outputQueue, received)
  diff(sent.toSet, received.toList, 1000)
  collectorThread.join()

  private def collectReceived(queue: BlockingQueue[ConsumerRecord[Long, String]],
                              received: mutable.MutableList[ReceivedMessage]) = {
    println("queue size: " + queue.size())
    var complete = false
    while(!complete){
      val record = queue.poll()
      if(record == null){
        complete = true
      } else {
        val expireMs = record.value().split(":")(0).toLong
        val messageId = record.value().split(":")(1).toInt
        received += ReceivedMessage(expireMs, record.timestamp(), messageId)
      }
    }
  }

  private def diff(sent: Set[SendMessage],
                   received: List[ReceivedMessage],
                   maxDiffMs: Long): Unit = {
    println("------- check begin ----------")
    if(sent.size != received.size) {
      println(s"incorrect message count, send: ${sent.size}, received: ${received.size}")
    } else
      println(s"received ${received.size} messages")
    val allMessages = mutable.Map.empty[Int, SendMessage]
    sent.foreach { m =>
      allMessages.put(m.id, m)
    }
    var sumDiff = 0L
    var maxDiff = 0L
    var minDiff = 0L
    received.foreach { m =>
      if(!allMessages.contains(m.id)){
        println("find message never send or duplicated" + m)
      }
      val origin = allMessages(m.id)
      if(origin.expireMs != m.expireMs){
        println(s"origin expireMs ${origin.expireMs} diff from received expireMs ${m.expireMs}")
      }
      val diff = Math.abs(origin.expireMs -m.messageTs)
      sumDiff = sumDiff + diff
      if(diff > maxDiff) maxDiff = diff
      if(diff < minDiff) minDiff = diff
      if(diff > maxDiffMs){
        val expirationMsStr = Utils.milliToString(origin.expireMs)
        println(s"time diff $diff to much, origin $origin received $m, expireMs: $expirationMsStr")
      }
      allMessages.remove(m.id)
    }
    if(!allMessages.isEmpty){
      println(s"some messages never consumed $allMessages")
    }  else {
      println("all messages match")
    }

    println(s"average diff: ${sumDiff/received.size}, max diff: $maxDiff, min diff: $minDiff")
    println("------ check complete ---------")


  }

}



class MessageCollector(baseTopic: String,
                       partitionNum: Int,
                       bootstrapServers: String,
                       outputQueue: BlockingQueue[ConsumerRecord[Long, String]],
                       latch: CountDownLatch) extends Runnable {
  private val running: AtomicBoolean = new AtomicBoolean(true)
  private val expiredTopic = TopicMapper.getExpiredTopicName(baseTopic)

  override def run() = {
    var consumerOpt: Option[KafkaConsumer[Long, String]] = None
    try {
      consumerOpt = Some(new KafkaConsumer[Long, String](properties))
      val consumer = consumerOpt.get
      consumer.subscribe(util.Arrays.asList(expiredTopic))
      var assigned = false
      while(!assigned){
        consumer.poll(100)
        val assignedPartitions = consumer.assignment()
        if(assignedPartitions.size() == partitionNum){
          assigned = true
          println("assigned " + assignedPartitions.size() + " partitions")
        }
      }
      while (running.get()) {
        val records = consumer.poll(100)
        if (latch.getCount > 0)
          latch.countDown()
        val recordIterator = records.iterator()
        while (recordIterator.hasNext) {
          val record = recordIterator.next()
          outputQueue.put(record)
        }
      }
    } finally {
      consumerOpt.foreach(_.close)
    }
  }

  def shutdown() = running.set(false)

  private def properties: Properties = {
    val config = new Properties()
    config.put(ConsumerConfig.CLIENT_ID_CONFIG, "delay-service-test")
    config.put(ConsumerConfig.GROUP_ID_CONFIG, "delay-service-test-delivery-test2")
    config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false")
    config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[LongDeserializer].getCanonicalName)
    config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getCanonicalName)
    config
  }
}