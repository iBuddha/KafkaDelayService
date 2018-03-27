package kafka.delay.test.unit.timer

import kafka.delay.message.timer.SystemMessageTimer

object TimerHelper {
  def drainTimer(timer: SystemMessageTimer, timeoutMs: Long): Unit = {
//    while(timer.advanceClock(timeoutMs)){}
    timer.advanceClock(timeoutMs)
  }
}
