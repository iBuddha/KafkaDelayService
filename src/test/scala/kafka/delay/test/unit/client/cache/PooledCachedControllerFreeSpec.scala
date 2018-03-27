package kafka.delay.test.unit.client.cache

import akka.actor.ActorRef
import kafka.delay.message.actor.metrics.CacheController.CacheState
import kafka.delay.message.actor.metrics.PooledCachedController
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FlatSpec, Matchers}

class PooledCachedControllerFreeSpec extends FlatSpec with Matchers {
  import PooledCachedControllerFreeSpec._
  //当释放完超过cache最小大小之后，仍不足以满足需要时
  "controller" should "free when space above mini-size is enough" in {
    val controller = new PooledCachedController(1000, 100)
    controller.update(CacheState(0, tpA, sender, 500, 0, None))
    controller.update(CacheState(0, tpB, sender, 500, 0, Some(0.1)))
    controller.tryFree(100, 400, 0.1) shouldBe 100
    //此时，各个cache的大小为400, 500
    controller.getState(tpA).get.currentSize shouldBe 400
    controller.getState(tpB).get.currentSize shouldBe 500
    controller.tryFree(200, 300, 0.1) shouldBe 200
    //此时，各个cache的大小为： 300, 400
    controller.getState(tpA).get.currentSize shouldBe 300
    controller.getState(tpB).get.currentSize shouldBe 400
  }

  "controller" should "free no more when mini size is met but no more extra space from less effective caches" in {
    val controller = new PooledCachedController(1000, 100)
    controller.update(CacheState(1, tpA, sender, 500, 0, Some(0.3)))
    controller.update(CacheState(1, tpB, sender, 500, 0, Some(0.5)))
    controller.update(CacheState(1, tpC, sender, 200, 0, Some(0.7)))
    controller.tryFree(500, 300, 0.1) shouldBe 300
    //此时，各个cache的大小为： 300, 400
    controller.getState(tpA).get.currentSize shouldBe 300
    controller.getState(tpB).get.currentSize shouldBe 400
    controller.getState(tpC).get.currentSize shouldBe 200
  }

  "controller" should "free space from less effective caches" in {
    val controller = new PooledCachedController(2000, 100)
    controller.update(CacheState(1, tpA, sender, 500, 0, Some(0.3)))
    controller.update(CacheState(1, tpB, sender, 500, 0, Some(0.5)))
    controller.update(CacheState(1, tpC, sender, 500, 0, Some(0.7)))
    controller.tryFree(1000, 200, 0.6) shouldBe 600
    controller.getState(tpA).get.currentSize shouldBe 200
    controller.getState(tpB).get.currentSize shouldBe 200
    controller.getState(tpC).get.currentSize shouldBe 500
    //此时 tpA大小为200， tpB大小为200, tpC大小为500
    controller.tryFree(400, 200, 0.8) shouldBe 300
    controller.getState(tpC).get.currentSize shouldBe 200

  }
}

object PooledCachedControllerFreeSpec {
  val tpA = new TopicPartition("tpA", 0)
  val tpB = new TopicPartition("tpB", 0)
  val tpC = new TopicPartition("tpC", 0)
  val tpD = new TopicPartition("tpD", 0)
  val sender = ActorRef.noSender
}
