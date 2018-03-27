package kafka.delay.test.unit.actor

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.delay.message.actor.TopicDelayService
import kafka.delay.message.actor.cached.RecordCacheActor
import kafka.delay.message.actor.request._
import kafka.delay.message.client.cache.RecordCache
import kafka.delay.message.client.parser.KeyBasedRecordExpireTimeParser
import kafka.delay.message.timer.meta.{ArrayOffsetBatch, OffsetBatchList}
import kafka.delay.test.unit.kafka.KafkaUtils
import org.apache.kafka.common.TopicPartition
import org.scalamock.scalatest.MockFactory
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

class RecordCacheActorSpec extends TestKit(ActorSystem("test-system"))
  with FlatSpecLike
  with Matchers
  with MockFactory
  with BeforeAndAfterAll
  with ImplicitSender {

  "RecordCacheActor" should "add" in {
    val baseTopic = "foo"
    val partition = 0
    implicit val tp = new TopicPartition(baseTopic, partition)
    val cacheController = testActor
    val expiredSender = testActor
    val cacheActor =
      system.actorOf(
        Props(
          new RecordCacheActor(
            baseTopic,
            partition,
            null,
            cacheController,
            RecordCache.InitialRecordCacheSize,
            TopicDelayService.RecordCacheExpireMs,
            TopicDelayService.RecordCachePaddingMs,
            new KeyBasedRecordExpireTimeParser
          )
        ),
        s"record-cache-$baseTopic-$partition")
    val cacheActorOffsets = (10L until 20)
    val records = cacheActorOffsets.map { offset => KafkaUtils.newLongKeyRecord(offset, System.currentTimeMillis() + 10000)}
    val addCacheRequest = CacheAdd(records.toList)
    cacheActor ! addCacheRequest
    cacheActor ! GetCached(OffsetBatchList(List(new ArrayOffsetBatch(Array(12L, 13)))), 0)

    val updateRequest = expectMsgType[UpdateCacheStateRequest]
    var getCachedResponse = expectMsgType[CachedRecords]
    getCachedResponse.records.map(_.offset()) shouldEqual Seq(12L, 13)
    getCachedResponse.requestId shouldBe 0

    cacheActor ! CacheRemove(Array(12, 13L))
    cacheActor ! GetCached(OffsetBatchList(List(new ArrayOffsetBatch(Array(13L, 14)))), 1)
    getCachedResponse = expectMsgType[CachedRecords]
    getCachedResponse.records.map(_.offset()) shouldEqual Seq(14L)
    getCachedResponse.requestId shouldBe 1
  }

  override def afterAll: Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }

}
