package kafka.delay.message.timer.bucket

import kafka.delay.message.timer.meta.DelayMessageMeta
import kafka.delay.message.utils.Utils

import scala.collection.SortedSet

/**
  * 代表压缩后的DelayMessageMeta数组
  * @param offsetUnit
  * @param absExpireMsUnit
  */
class CompactedMetaArray(offsetUnit: SortedCompactedLongArray,
                         absExpireMsUnit: UnSortedCompressedLongArray) extends Iterable[DelayMessageMeta] {

  require(offsetUnit.length ==  absExpireMsUnit.length)

  val length = offsetUnit.length
  val sizeInByte = offsetUnit.sizeInByte + absExpireMsUnit.sizeInByte

  def unCompress(): Array[DelayMessageMeta] = {
    val uncompressedOffsets = offsetUnit.uncompact()
    val uncompressedAbsExpires = absExpireMsUnit.uncompact()
    val length = uncompressedOffsets.length
    val metas = new Array[DelayMessageMeta](length)
    var i = 0
    while (i < length) {
      metas(i) = new DelayMessageMeta(uncompressedOffsets(i), uncompressedAbsExpires(i))
      i += 1
    }
    metas
  }

  override def iterator = this.unCompress().iterator
}

object CompactedMetaArray {
  def apply(metas: Array[DelayMessageMeta]): CompactedMetaArray = {
    val sortedByOffset = sortAndRemoveDuplicated(metas)
    val length = sortedByOffset.length
    val offsets = new Array[Long](length)
    val absExpireMs = new Array[Long](length)
    var i = 0
    while(i < length){
      val e = sortedByOffset(i)
      offsets(i) = e.offset
      absExpireMs(i) = e.expirationMsAbsolutely
      i += 1
    }
    val offsetUnit = {
      val bitmapBased = SortedBitmapBasedLongArray(offsets)
      val compressionBased = SortedCompressedLongArray(offsets)
      if(bitmapBased.sizeInByte < compressionBased.sizeInByte)
        bitmapBased
      else
        compressionBased
    }
    val absExpireMsUnit = UnSortedCompressedLongArray(absExpireMs)
    new CompactedMetaArray(offsetUnit, absExpireMsUnit)
  }

  /**
    * 排序，并且移除offset重复的元素。
    * 假设情况为通常并不会有重复的元素(重复元素只可能在consumer重启时才会发生)，并据此进行优化
    * @param metas
    * @return
    */
  def sortAndRemoveDuplicated(metas: Array[DelayMessageMeta]): Array[DelayMessageMeta] = {
    Utils.sortedAndRemoveDuplicated(metas)
  }
}
