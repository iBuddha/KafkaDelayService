package kafka.delay.test.unit.utils

import java.sql.Timestamp

import org.scalatest.{FlatSpecLike, Matchers}

object TestUtils extends Matchers {
  def timeShouldWithin(maxDiff: Long, firstTime: Long, secondTime: Long) = {
    if (Math.max(maxDiff, Math.abs(firstTime - secondTime)) != maxDiff)
      println(s"firstTime: ${timeToString(firstTime) }, secondTime: ${timeToString(secondTime) }")
    Math.max(maxDiff, Math.abs(firstTime - secondTime)) shouldBe maxDiff
  }

  def timeToString(timestamp: Long) = new Timestamp(timestamp).toString
}
