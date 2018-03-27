package kafka.delay.test.unit.client.cache

import java.util.concurrent.TimeUnit

import akka.actor.{ActorSystem, Props}
import akka.testkit.{ImplicitSender, TestKit}
import kafka.delay.message.actor.metrics.CacheController.CacheState
import kafka.delay.message.actor.metrics.CacheControllerActor
import kafka.delay.message.actor.request._
import org.apache.kafka.common.TopicPartition
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}

import scala.concurrent.duration.FiniteDuration
import ControllerActorSpec.{CheckInterval, _}

class ControllerActorSpec extends TestKit(ActorSystem("test-system"))
  with FlatSpecLike
  with Matchers
  with ImplicitSender
  with BeforeAndAfterAll {

  "controller actor" should "response to meta request" in {
    val controller = system.actorOf(Props(new CacheControllerActor(1000, 10, CheckInterval)))
    controller ! GetCacheControllerMetaRequest
    val response = expectMsgType[CacheControllerMetaResponse]
    response shouldEqual CacheControllerMetaResponse(1000, 10)
  }

  "controller actor" should "reset cache size when update request version is stale" in {
    val controller = system.actorOf(Props(new CacheControllerActor(1000, 10, CheckInterval)))
    val tp = new TopicPartition("test", 0)
    controller ! UpdateCacheStateRequest(CacheState(1, tp, testActor, 100, 0, None))
    controller ! UpdateCacheStateRequest(CacheState(2, tp, testActor, 200, 0, None))
    controller ! UpdateCacheStateRequest(CacheState(1, tp, testActor, 300, 0, None))
    expectMsg(SetCacheSizeRequest(200, 2))
  }

  "controller actor" should "update" in {
    val controller = system.actorOf(Props(new CacheControllerActor(1000, 10,CheckInterval)))
    val tp = new TopicPartition("test", 0)
    controller ! UpdateCacheStateRequest(CacheState(1, tp, testActor, 100,0,  None))
    controller ! UpdateCacheStateRequest(CacheState(2, tp, testActor, 200, 0, None))
    expectNoMsg(FiniteDuration(1, TimeUnit.SECONDS))
  }

  "controller actor" should "reset cache size when assign request version is stale" in {
    val controller = system.actorOf(Props(new CacheControllerActor(1000, 10, CheckInterval)))
    val tp = new TopicPartition("test", 0)
    controller ! UpdateCacheStateRequest(CacheState(1, tp, testActor, 100, 0, None))
    controller ! CacheSpaceRequest(tp, testActor, 200, 0.1, 0)
    expectMsg(SetCacheSizeRequest(100, 1))
  }

  "controller actor" should "assign" in {
    val controller = system.actorOf(Props(new CacheControllerActor(1000, 10, CheckInterval)))
    val tp = new TopicPartition("test", 0)
    controller ! CacheSpaceRequest(tp, testActor, 100, 0.1, 1)
    expectMsg(SetCacheSizeRequest(100, 2))
    controller ! CacheSpaceRequest(tp, testActor, 200, 0.1, 2)
    expectMsg(SetCacheSizeRequest(200, 3))
    controller ! CacheSpaceRequest(tp, testActor, 300, 0.1, 2)
    expectMsg(SetCacheSizeRequest(200, 3))
    controller ! CacheSpaceRequest(tp, testActor, 1200, 0.1, 3)
    expectMsg(SetCacheSizeRequest(1000, 4))
  }

  override def afterAll(): Unit = {
    TestKit.shutdownActorSystem(system)
    super.afterAll()
  }
}

object ControllerActorSpec {
  val CheckInterval = FiniteDuration(10, TimeUnit.SECONDS)
}

