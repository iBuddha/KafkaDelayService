package kafka.delay.message.timer.meta

trait OffsetBatchBuilder {
  def add(offset: Long): Boolean
  def build(): OffsetBatch
}

object OffsetBatchBuilder {

  //how full to use bitmap base batch
  private val bitmapRatio = 0.9
  private val bitmapMinLength = 1024

  def getBuilder(offsets: Seq[Long], min: Long, range: Long, maxDistance: Long): OffsetBatchBuilder = {
    val offsetMin = offsets.head
    val offsetMax = offsets.last
    val inputRange = offsetMax - offsetMin + 1
    if(offsets.length > bitmapMinLength && (offsets.length > inputRange * bitmapRatio)){
      new BitmapOffsetBatchBuilder(range, min, maxDistance )
    } else{
      new ArrayOffsetBatchBuilder(range, min, maxDistance)
    }
  }
}
