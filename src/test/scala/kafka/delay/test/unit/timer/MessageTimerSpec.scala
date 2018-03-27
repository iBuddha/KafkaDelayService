package kafka.delay.test.unit.timer

import java.sql.Timestamp
import java.util.concurrent.{BlockingQueue, Executors, LinkedBlockingQueue}

import kafka.delay.message.timer.SystemMessageTimer
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.SystemTime
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by xhuang on 19/07/2017.
  */
class MessageTimerSpec extends FlatSpec with Matchers {

  "MessageTimer" should "delay single message" in {
    delaySingleMessage(5)
    delaySingleMessage(12)
    delaySingleMessage(30)
  }

  "MessageTimer" should "delay multiple messages " in {
    val messageQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    val addTime = time.milliseconds
    val timer = new SystemMessageTimer(messageQueue, 10, 3, SystemTime.hiResClockMs)
    val delayTimes = List(200L, 1000L, 6000L, 11000L)
    delayTimes.foreach { delay =>
      val delayTo = delay + addTime
      timer.add(mockDelayMessageMeta(delayTo))
      println(s"add delay message, expire at " + milliToString(delayTo))
    }
    delayTimes.foreach { delay =>
      val message = drainMessage(200, timer, messageQueue)
      val currentTime = time.milliseconds
      println(s"get message: $message, now " + milliToString(currentTime) )
      timeShouldWithin(1200, addTime + delay, currentTime)
      timeShouldWithin(1100, message.expirationMsAbsolutely, time.hiResClockMs)
    }
  }

  "MessageTimer" should "delay multiple messages from multiple threads" in {
    class DelayTask(expireMs: Long, timer: SystemMessageTimer) extends Runnable {
      override def run(): Unit = {
        val messageMeta = mockDelayMessageMeta(expireMs)
        timer.add(messageMeta)
        Thread.sleep(200)
      }
    }
    val executor = Executors.newFixedThreadPool(10)
    val delayCompleted = new LinkedBlockingQueue[DelayMessageMeta]()
    val timer = new SystemMessageTimer(delayCompleted, 100, 3, SystemTime.hiResClockMs)

    
    val delayMs = List(200, 1000, 1100, 3000, 3100, 10000, 10100, 11000)
    val addTime = time.milliseconds
    delayMs.foreach { delay =>
      val task = new DelayTask(addTime + delay, timer)
      executor.submit(task)
    }

    delayMs.foreach { delay =>
      val message = drainMessage(100, timer, delayCompleted)
      val currentTime = time.milliseconds
      println(s"get message: $message, now " + milliToString(currentTime) )
      timeShouldWithin(200, addTime + delay, currentTime)
      timeShouldWithin(200, message.expirationMsAbsolutely, time.hiResClockMs)
    }
    executor.shutdownNow()
  }

  private def delaySingleMessage(delaySeconds: Long) = {
    val delayMs = delaySeconds * 1000
    val messageQueue = new LinkedBlockingQueue[DelayMessageMeta]()
    val timer = new SystemMessageTimer(
      outQueue = messageQueue,
      tickMs = 100,
      wheelSize = 10,
      startMs = SystemTime.hiResClockMs)
    timer.add(mockDelayMessageMeta( time.milliseconds + delayMs))
    val message = drainMessage(100, timer, messageQueue)
    Math.max(200, Math.abs(message.expirationMsAbsolutely - time.hiResClockMs)) shouldBe 200
  }

  private def timeShouldWithin(maxDiff: Long, firstTime: Long, secondTime: Long) = {
    if (Math.max(maxDiff, Math.abs(firstTime - secondTime)) != maxDiff)
      println(s"firstTime: $firstTime, secondTime: $secondTime")
    Math.max(maxDiff, Math.abs(firstTime - secondTime)) shouldBe maxDiff
  }

  private def drainMessage(stepMs: Long,
                         timer: SystemMessageTimer,
                         outputQueue: BlockingQueue[DelayMessageMeta]): DelayMessageMeta = {
    var message: DelayMessageMeta = null
    while (message == null) {
      timer.advanceClock(stepMs)
      message = outputQueue.poll()
    }
    message
  }

  private val time = SystemTime

  private def mockDelayMessageMeta(expireMs: Long) = DelayMessageMeta.fromClockTime(1, expireMs, time)

  private def milliToString(millis: Long): String = new Timestamp(millis).toString

}
