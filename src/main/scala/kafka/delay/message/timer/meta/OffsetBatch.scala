package kafka.delay.message.timer.meta



/**
  * 本质上它是一个Seq[Long]而且是排序好的, 并且是非空的
  */
trait OffsetBatch extends scala.collection.immutable.Seq[Long] {
  def contains(offset: Long): Boolean

  /**
    * 截取大于或等于参数的其余部分
    * @param fromOffset inclusive
    * @return
    */
  def from(fromOffset: Long): Option[OffsetBatch]
}

//object MetaBatch {
//  val Empty: MetaBatch = new MetaBatch {
//    override def apply(idx: Int): Long = throw UnsupportedOperationException
//
//    override def length: Int = 0
//
//    override def iterator: Iterator[Long] = Iterator.empty
//
//    override def keysIteratorFrom(start: Long): Iterator[Long] = Iterator.empty
//
//    override def ordering: Ordering[Long] = Ordering[Long]
//
//    override def firstKey: Long = throw UnsupportedOperationException
//
//    override def rangeImpl(from: Option[Long], until: Option[Long]): MetaBatch = this
//
//    override def lastKey: Long = throw UnsupportedOperationException
//
//    override def keySet: collection.SortedSet[Long] = SortedSet.empty
//  }
//}