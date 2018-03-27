package kafka.delay.message.client

case class OffsetReset(value: String)
object OffsetReset {
  val earliest = new OffsetReset("earliest")
  val latest = new OffsetReset("latest")
}

