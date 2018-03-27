package kafka.delay.test.unit.utils

import kafka.delay.message.utils.{SystemTime, Time}

class MockTime(clockMs: Long, hiResMs: Long) extends Time {
  override def milliseconds: Long = clockMs

  override def nanoseconds: Long = hiResMs

  override def sleep(ms: Long): Unit = Thread.sleep(ms)
}
