package kafka.delay.test.unit.client.cache

import kafka.delay.message.actor.metrics.PooledCachedController
import org.apache.kafka.common.TopicPartition
import org.scalatest.{FlatSpec, Matchers}
import PooledCachedControllerSpec._
import akka.actor.ActorRef
import kafka.delay.message.actor.metrics.CacheController.CacheState

class PooledCachedControllerSpec extends FlatSpec with Matchers {
  "controller" should "allow update size smaller than min" in {
    val controller = new PooledCachedController(1024, 1024)
    controller.update(new CacheState(0, defaultTp, ActorRef.noSender, 1024, 0, None))
    controller.usedSize shouldBe 1024
    val newState = new CacheState(0, defaultTp, ActorRef.noSender, 0, 0, None)
    controller.update(newState)
    controller.getState(defaultTp).get shouldEqual newState
    controller.usedSize shouldBe newState.currentSize
  }

  "controller" should "update" in {
    val controller = new PooledCachedController(10240, 1024)
    val metricA = new CacheState(0, defaultTp, ActorRef.noSender, 1000, 0, None)
    val metricB = new CacheState(1, defaultTp, ActorRef.noSender, 800, 0, None)
    controller.update(metricA)
    controller.update(metricB)
    controller.getState(defaultTp) shouldEqual Some(metricB)
    controller.usedSize shouldBe 800
  }

  "controller" should "assign and update version when has enough free memory" in {
    val controller = new PooledCachedController(10240, 1024)
    controller.update(new CacheState(0, defaultTp, ActorRef.noSender, 1000, 0, None))
    val result = controller.assign(0, defaultTp, ActorRef.noSender, 2048, 0.5)
    result.size shouldBe 1
    result(defaultTp).currentSize shouldBe 2048
  }


  "controller" should "assign when not assignable" in {
    val controller = new PooledCachedController(1024, 1024)
    controller.update(new CacheState(0, defaultTp, ActorRef.noSender, 1024, 0, None))
    val tp = new TopicPartition("foo", 1)
    val result = controller.assign(0, tp, ActorRef.noSender, 1024, 0.5)
//    result.left.get shouldBe 0
    result.size shouldBe 1
    result(tp).currentSize shouldBe 0
    result(tp).version shouldBe 1
  }

  "controller" should "assign when tp ask to shrink" in {
    val controller = new PooledCachedController(1024, 1024)
    controller.update(new CacheState(0, defaultTp, ActorRef.noSender, 1024, 0, None))
    val result = controller.assign(0, defaultTp, ActorRef.noSender, 1000, 0.5)
    result(defaultTp).currentSize shouldBe 1000
    result(defaultTp).version shouldBe 1
    controller.usedSize shouldBe 1000
  }

  "controller" should "use free space first" in {
    val controller = new PooledCachedController(3000, 500)
    controller.assign(0, tpD, ActorRef.noSender, 800, 0.1)(tpD).currentSize shouldBe 800
    controller.assign(0, tpD, ActorRef.noSender, 0, 0.1)
    controller.update(CacheState(0, tpA, ActorRef.noSender, 800, 0, Some(0.3)))
    controller.update(CacheState(0, tpB, ActorRef.noSender, 1000, 0, Some(0.5)))
    controller.update(CacheState(0, tpC, ActorRef.noSender, 1000, 0, Some(0.7)))
    val effected  = controller.assign(1, tpD, ActorRef.noSender, 500, 0.1)
    effected(tpA).currentSize shouldBe 500
    effected(tpA).version shouldBe 1
    effected(tpD).currentSize shouldBe 500
    effected(tpD).version shouldBe 2
  }

  "controller" should "squeeze space" in {
    var controller = new PooledCachedController(3000, 1000)

    controller.update(CacheState(0, tpA, ActorRef.noSender, 1000, 0, Some(0.4)))
    controller.update(CacheState(0, tpB, ActorRef.noSender, 1000, 0, Some(0.5)))
    controller.update(CacheState(0, tpC, ActorRef.noSender, 1000, 0, Some(0.5)))
    controller.assign(0, tpD, ActorRef.noSender, 1100, 0.3)(tpD).currentSize shouldBe 0


    controller.update(CacheState(0, tpA, ActorRef.noSender, 1400, 0, Some(0.4)))
    controller.update(CacheState(0, tpB, ActorRef.noSender, 1600, 0, Some(0.5)))
    controller.update(CacheState(0, tpC, ActorRef.noSender, 0, 0, Some(0.5)))

    var result = controller.assign(0, tpD, ActorRef.noSender, 1100, 0.3)
//    result should be ('right)
    result(tpA).currentSize shouldBe 1000
    result(tpB).currentSize shouldBe 1000
    result(tpD).currentSize shouldBe 1000

    controller = new PooledCachedController(3000, 200)
    controller.update(CacheState(0, tpA, ActorRef.noSender, 200, 0, Some(0.3)))
    controller.update(CacheState(0, tpB, ActorRef.noSender, 1700, 0, Some(0.5)))
    controller.update(CacheState(0, tpC, ActorRef.noSender, 1000, 0, Some(0.7)))

    result = controller.assign(0, tpD, ActorRef.noSender, 800, 0.55)
    result(tpB).currentSize shouldBe 1000
    result(tpD).currentSize shouldBe 800
  }
}

object PooledCachedControllerSpec {
  val defaultTp = new TopicPartition("t9e12s123t", 1)
  val tpA = new TopicPartition("tp1", 0)
  val tpB = new TopicPartition("tp2", 0)
  val tpC = new TopicPartition("tp3", 0)
  val tpD = new TopicPartition("tp4", 0)
}
