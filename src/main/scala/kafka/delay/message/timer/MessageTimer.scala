/**
  * Licensed to the Apache Software Foundation (ASF) under one or more
  * contributor license agreements.  See the NOTICE file distributed with
  * this work for additional information regarding copyright ownership.
  * The ASF licenses this file to You under the Apache License, Version 2.0
  * (the "License"); you may not use this file except in compliance with
  * the License.  You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */
package kafka.delay.message.timer

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.ReentrantReadWriteLock

import kafka.delay.message.timer.bucket.TimerBucket
import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.SystemTime
import kafka.utils.threadsafe
import org.slf4j.LoggerFactory

trait MessageTimer {
  /**
    * Add a new task to this executor. It will be executed after the task's delay
    * (beginning from the time of submission)
    */
  def add(messageMeta: DelayMessageMeta): Unit

  /**
    * Advance the internal clock, executing any tasks whose expiration has been
    * reached within the duration of the passed timeout.
    *
    * @param timeoutMs
    * @return whether or not any tasks were executed
    */
  def advanceClock(timeoutMs: Long): Boolean

  /**
    * Get the number of tasks pending execution
    *
    * @return the number of tasks
    */
  def size: Long

  /**
    * Shutdown the timer service, leaving pending tasks unexecuted
    */
  def shutdown(): Unit
}

/**
  *
  * @param outQueue  expired message meta将会被输出到这个queue里
  * @param tickMs    每个tick的大小，这个决定了timer的精度。也影
  * @param wheelSize wheel的大小，决定了时间复杂度
  * @param startMs   timer初始化时使用的时间，它决定了最初的wheel的第一个bucket对应的时间
  */
@threadsafe
class SystemMessageTimer(val outQueue: BlockingQueue[DelayMessageMeta],
                         tickMs: Long = 1000,
                         wheelSize: Int = 60,
                         startMs: Long = SystemTime.hiResClockMs) extends MessageTimer {

  import SystemMessageTimer._
  // timeout timer
  //  private[this] val taskExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
  //    def newThread(runnable: Runnable): Thread =
  //      Utils.newThread("executor-"+executorName, runnable, false)
  //  })
  //  private[this] val outQueue: BlockingQueue[DelayMessageMeta] = new LinkedBlockingQueue[DelayMessageMeta]()

  private[this] val delayQueue = new DelayQueue[TimerBucket]()
  private[this] val messageCounter = new AtomicLong(0)
  private[this] val timingWheel = new TimingWheel(
    tickMs = tickMs,
    wheelSize = wheelSize,
    startMs = startMs,
    taskCounter = messageCounter,
    delayQueue
  )

  // Locks used to protect data structures while ticking
  private[this] val readWriteLock = new ReentrantReadWriteLock()
  private[this] val readLock = readWriteLock.readLock()
  private[this] val writeLock = readWriteLock.writeLock()

  def add(delayMessage: DelayMessageMeta): Unit = {
    readLock.lock()
    try {
      addMessage(delayMessage)
    } finally {
      readLock.unlock()
    }
  }

  def add(delayMessages: Traversable[DelayMessageMeta]): Unit = {
    readLock.lock()
    try {
      for (message <- delayMessages)
        addMessage(message)
    } finally {
      readLock.unlock()
    }
  }

  @inline
  private def addMessage(messageMeta: DelayMessageMeta): Unit = {
    if (!timingWheel.add(messageMeta)) {
      // Already expired or cancelled
      outQueue.put(messageMeta)
    }
  }

  private[this] val reinsert = (messageMeta: DelayMessageMeta) => addMessage(messageMeta)

  /*
   * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
   * waits up to timeoutMs before giving up.
   */
  def advanceClock(timeoutMs: Long): Boolean = {
    var bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS)
    if (bucket != null) {
      writeLock.lock()
      try {
        while (bucket != null) {
          timingWheel.advanceClock(bucket.getExpiration())
          bucket.flush(reinsert)
          bucket = delayQueue.poll()
        }
      } finally {
        writeLock.unlock()
      }
      true
    } else {
      false
    }
  }

  def size: Long = messageCounter.get

  override def shutdown() {
  }

}

object SystemMessageTimer {
  val logger = LoggerFactory.getLogger(SystemMessageTimer.getClass)
}

