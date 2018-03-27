package kafka.delay.test.unit.timer

class TimeUtil(value: Long) {

  def diff(second: Long): Long = {
    Math.abs(value - second)
  }

  def withIn(range: Long): Boolean = {
    Math.max(value, range)  == range
  }
}

object TimeUtil{
  implicit def apply(value: Long): TimeUtil = new TimeUtil(value)
}