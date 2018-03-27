package kafka.delay.test.unit.storage

import kafka.delay.message.storage.AbstractKafkaMetaStorage.{MetaKey, MetaValue}
import kafka.delay.message.storage.BitmapKafkaMetaStorage
import kafka.delay.message.timer.MessageTimer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.{SystemTime, Time, TopicMapper}
import kafka.delay.test.unit.kafka.broker.WithSingleTopicBroker
import kafka.delay.test.unit.kafka.consumer.ArrayBackedMetaConsumer
import kafka.delay.test.unit.utils.MockTime
import org.apache.kafka.clients.consumer.{KafkaConsumer, OffsetResetStrategy}
import org.mockito.Mock
import org.scalamock.scalatest.MockFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.util.Random

class BitmapKafkaMetaStorageSpec extends FlatSpec
  with Matchers
//  with WithSingleTopicBroker
  with MockFactory {
  "BitMapKafkaMetaStorage" should "only add meta message with related delay topic offset" +
    " before specified max offset" in {
    val baseTopic = "test"
    val storage = new BitmapKafkaMetaStorage("localhost:9092", baseTopic, SystemTime)
    //add messages to ignore, because they are too old
    val consumer = new ArrayBackedMetaConsumer(
      TopicMapper.getMetaTopicName(baseTopic),
      partitionNumber = 3,
      OffsetResetStrategy.EARLIEST,
      maxPollRecords = 2)
    //expire time == delay topic offset == meta topic offset
    (0 until 100).foreach { index =>
      consumer.add(0, MetaKey(0, index), index.toLong)
    }

    val timer = mock[MessageTimer]
    (timer.add _).expects(where { messageMeta: DelayMessageMeta =>
      messageMeta.offset >= 10
    }).repeat(70)

    storage.restore(
      consumer = consumer,
      partitionId = 0,
      ignoreDelayTopicOffsetFrom = 80L,
      timer = timer,
      ignoredExpireBeforeMs = 10L,
      delayTopicLSO = 5L
    )
  }

  "BitMapKafkaMetaStorage" should "only add meta message with expireMs before specified value to timer" in {
    val mockTime = new Time {
      override def milliseconds: Long = 0

      override def nanoseconds: Long = 0

      override def sleep(ms: Long): Unit = SystemTime.sleep(ms)
    }
    val baseTopic = "test"
    val storage = new BitmapKafkaMetaStorage("localhost:9092", baseTopic, mockTime)
    //add messages to ignore, because they are too old
    val consumer = new ArrayBackedMetaConsumer(
      TopicMapper.getMetaTopicName(baseTopic),
      partitionNumber = 3,
      OffsetResetStrategy.EARLIEST,
      maxPollRecords = 2)
    //expire time == delay topic offset == meta topic offset
    (0 until 100).foreach { index =>
      consumer.add(0, MetaKey(0, index), index.toLong)
    }

    val timer = mock[MessageTimer]
    (timer.add _).expects(where { messageMeta: DelayMessageMeta =>
      (messageMeta.expirationMsAbsolutely >= 50  && messageMeta.expirationMsAbsolutely == messageMeta.offset) ||
      (messageMeta.expirationMsAbsolutely == 50 && messageMeta.offset == 100)

    }).atLeastOnce()

    storage.restore(
      consumer = consumer,
      partitionId = 0,
      ignoreDelayTopicOffsetFrom = 200L,
      timer = timer,
      ignoredExpireBeforeMs = 50,
      delayTopicLSO = 0L
    )
  }

  "BitMapKafkaMetaStorage" should "only add meta message whose associated delay message offset smaller bigger or equal" +
    "with delayTopic's begin offset" +
    " before specified max offset" in {
    val baseTopic = "test"
    val storage = new BitmapKafkaMetaStorage( "localhost:9092", baseTopic, SystemTime)
    //add messages to ignore, because they are too old
    val consumer = new ArrayBackedMetaConsumer(
      TopicMapper.getMetaTopicName(baseTopic),
      partitionNumber = 3,
      OffsetResetStrategy.EARLIEST,
      maxPollRecords = 2)
    //expire time == delay topic offset == meta topic offset
    (0 until 100).foreach { index =>
      consumer.add(0, MetaKey(0, index), index.toLong)
    }

    val timer = mock[MessageTimer]
    (timer.add _).expects(where { messageMeta: DelayMessageMeta =>
      messageMeta.offset >= 20
    }).repeat(60)

    storage.restore(
      consumer = consumer,
      partitionId = 0,
      ignoreDelayTopicOffsetFrom = 80L,
      timer = timer,
      ignoredExpireBeforeMs = 10L,
      delayTopicLSO = 20L
    )
  }

  "BitMapKafkaMetaStorage" should "have basic performance" in {
    val mockTime = new MockTime(0, 0)
    val baseTopic = "test"
    val storage = new BitmapKafkaMetaStorage("localhost:9092", baseTopic, mockTime)
    //add messages to ignore, because they are too old
    val consumer = new ArrayBackedMetaConsumer(
      TopicMapper.getMetaTopicName(baseTopic),
      partitionNumber = 3,
      OffsetResetStrategy.EARLIEST,
      maxPollRecords = 2)
    //expire time == delay topic offset == meta topic offset
    (0 until 1000000).foreach { index =>
      consumer.add(0, MetaKey(0, index), index.toLong)
    }

    val timer = mock[MessageTimer]
    (timer.add _).expects(where { messageMeta: DelayMessageMeta =>
      messageMeta.expirationMsAbsolutely == messageMeta.offset
    }).atLeastOnce()

    val startTime = System.currentTimeMillis()
    storage.restore(
      consumer = consumer,
      partitionId = 0,
      ignoreDelayTopicOffsetFrom = 200L,
      timer = timer,
      ignoredExpireBeforeMs = 50,
      delayTopicLSO = 0L
    )
    val endTime = System.currentTimeMillis()
    assert(endTime - startTime < 10000)
  }

  "BitMapKafkaMetaStorage" should "remove completed delay message" in {
    val baseTopic = "test"
    val storage = new BitmapKafkaMetaStorage( "localhost:9092", baseTopic, SystemTime)
    //add messages to ignore, because they are too old
    val consumer = new ArrayBackedMetaConsumer(
      TopicMapper.getMetaTopicName(baseTopic),
      partitionNumber = 3,
      OffsetResetStrategy.EARLIEST,
      maxPollRecords = 2)
    //expire time == delay topic offset == meta topic offset
    (0 until 100).foreach { index =>
      consumer.add(0, MetaKey(0, index), index.toLong)
    }

    (20 until 30).foreach {index =>
      consumer.add(0, MetaKey(0, index), null)
    }

    val timer = mock[MessageTimer]
    (timer.add _).expects(where { messageMeta: DelayMessageMeta =>
      messageMeta.offset < 20 || messageMeta.offset >= 30
    }).repeat(60)

    storage.restore(
      consumer = consumer,
      partitionId = 0,
      ignoreDelayTopicOffsetFrom = 80L,
      timer = timer,
      ignoredExpireBeforeMs = 10L,
      delayTopicLSO = 0L
    )
  }
//  override protected def topicName: String = TopicMapper.getMetaTopicName("test")
//
//  override protected def partitionNum: Int = 3
}

/**
  *consumer.assign(targetTp)
  *consumer.poll(1000)
  *consumer.unsubscribe()
  *consumer.seekToBeginning(targetTp)
  * while (!completed) {
  * val records = consumer.poll(1000)
  * *
  *consumer.assign(util.Arrays.asList(metaTp))
  * val beginOffset = consumer.beginningOffsets(util.Arrays.asList(metaTp)).get(metaTp)
  * val endOffset = consumer.endOffsets(util.Arrays.asList(metaTp)).get(metaTp)
  **/
