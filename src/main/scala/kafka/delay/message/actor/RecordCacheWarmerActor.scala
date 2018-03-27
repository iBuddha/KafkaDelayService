package kafka.delay.message.actor

import java.util
import java.util.Properties

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import kafka.delay.message.actor.request._
import kafka.delay.message.client.ClientCreator
import kafka.delay.message.timer.meta.{OffsetBatch, OffsetBatchList}
import kafka.delay.message.utils.TopicMapper
import org.apache.kafka.clients.consumer.{Consumer, ConsumerRecord}
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable


/**
  * 用于提前预热record cache。
  * 实际上添加了一种基于cache的并发读取的方式。以规避kafka随机读性能差的问题
  */
class RecordCacheWarmerActor(bootstrapServers: String,
                             baseTopic: String,
                             partition: Int,
                             clientCreator: ClientCreator,
                             properties: Properties) extends Actor with ActorLogging {

  private[this] var consumer: Option[Consumer[Array[Byte], Array[Byte]]] = None
  private val messageTp = new TopicPartition(TopicMapper.getDelayTopicName(baseTopic), partition)

  private val warmerActorNumber = 50
  private var warmerActors: Option[Array[ActorRef]] = None

  override def preStart(): Unit = {
    consumer = Some(clientCreator.getConsumer(properties))
    consumer.get.assign(util.Arrays.asList(messageTp))
    log.debug("cache warmer started for {}", messageTp)
    super.preStart()
    val warmers = new mutable.ArrayBuffer[ActorRef](warmerActorNumber)
    (0 until warmerActorNumber).foreach { i =>
      warmers += context.actorOf(
        Props(new TempMessageConsumerActor(bootstrapServers, baseTopic, partition))
          .withDispatcher(Dispatchers.MessageConsumerDispatcher))
    }
    warmerActors = Some(warmers.toArray)
  }

  override def postStop(): Unit = {
    consumer.foreach(_.close())
    super.postStop()
  }


  override def receive = {
    case PreFetchBatchListRequest(batches) => {
      //      context.parent ! GetNonCachedOffsets(batches.batches)
      log.debug("got prefetch request with size {}", batches.batches.map(_.size).sum)
      val now = System.currentTimeMillis()
      if (batches.batches.size >= 3) {
        log.debug("{} batches in one batchList, so call redirect to warmerActors", batches.batches.size)
        batches.batches.zipWithIndex.foreach { e =>
          val warmerRef = warmerActors.get.apply(e._2 % warmerActorNumber)
          warmerRef ! Warm(context.parent, e._1, now)
        }
      } else {
        batches.batches.foreach(consume)
      }
    }

    case NonCachedOffsets(batches) => {
      batches.foreach { batch =>
        consume(batch)
      }
    }

    case BatchListConsumeRequest(batchList, request) =>
      batchList.batches.foreach(consume)
  }

  //      batches.batches.foreach { batch =>
  //        consumer(batch)
  //      }
  //      log.debug("prefetched {} records", batches.batches.map(_.size).sum)
  //    }

  private def consume(batch: OffsetBatch): Unit = {
    if (!batch.isEmpty) {
      val buffer = mutable.ListBuffer.empty[ConsumerRecord[Array[Byte], Array[Byte]]]
      consumer.get.seek(messageTp, batch.head)
      val max = batch.last
      var complete = false
      val pollInterval = 200
      val maxEmptyPoll = 5000 / pollInterval
      var currentEmptyPoll = 0
      while (!complete) {
        val records = consumer.get.poll(pollInterval)
        if (records.isEmpty) {
          currentEmptyPoll += 1
          if (currentEmptyPoll > maxEmptyPoll) {
            complete = true
          }
        } else {
          val iterator = records.iterator()
          while (iterator.hasNext && !complete) {
            val record = iterator.next()
            if (record.offset() >= max) {
              complete = true
            }
            buffer += record
          }
        }
      }
      val fetched = buffer.toList
      log.debug("pre consumed {} records with batch size {}", fetched.size, batch.size)
      context.parent ! CacheAdd(fetched)
    }
  }
}
