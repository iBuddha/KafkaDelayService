package kafka.delay.message.exception

class ImpossibleBatchException(message: String, lso: Long, leo: Long)
  extends RuntimeException(s"$message, LSO: $lso, LEO:$leo")
