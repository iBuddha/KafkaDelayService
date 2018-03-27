package kafka.delay.message.actor.metrics

import akka.actor.ActorRef
import kafka.delay.message.actor.metrics.CacheController.CacheState
import org.apache.kafka.common.TopicPartition

import scala.collection.mutable


/**
  * 需要考虑到，cache可能会把自己缩小到小于controller的minSize
  *
  * 把version相关的东西加到actor里
  *
  * @param maxSize 所有cache加起来的最大大小
  */
class PooledCachedController(maxSize: Int, miniCacheSize: Int) extends CacheController {
  private val caches: mutable.Map[TopicPartition, CacheState] = mutable.Map.empty

  override def usedSize = caches.map(_._2.currentSize).sum


  /**
    *
    * @param version          依据哪个cache的状态版本做出的决定
    * @param tp               提出申请的cache的TopicPartition
    * @param actor
    * @param targetSize
    * @param performancePoint cache的效用评分。所有cache都用同样的方法评估。只可用于排序，此值的绝对值的比例关系并不对应实际效用的比较关系。
    *                         (总的来说，就像是评分体系，比如1星至5星，并不意味着5星的效率是1星的5倍)
    * @return Left对应于只需要调整所这个cache的大小，Left的值表示调整后的大小。
    *         Right表示有其它cache的大小需要调整，其值表示进行调整的cache调整后的大小
    */
  override def assign(version: Long,
                      tp: TopicPartition,
                      actor: ActorRef,
                      targetSize: Int,
                      performancePoint: Double):  Map[TopicPartition, CacheState] = {
    val freeSpace = maxSize - usedSize
    val neededSpace = caches.get(tp).fold(targetSize)(targetSize - _.currentSize) - freeSpace
    if (neededSpace <= 0) {
      val point = caches.get(tp).flatMap(_.point)
      caches.update(tp, CacheState(version + 1, tp, actor, targetSize, 0, point))
      Map(tp -> caches(tp))
    } else {
      //we need to grab some space from others
      val preSnapshot = caches.toMap
      val freed = tryFree(neededSpace, miniCacheSize, performancePoint)
      val currentSize = caches.get(tp).fold(freed)(freed + _.currentSize) + freeSpace
      caches.update(tp, CacheState(version + 1, tp, actor, currentSize, 0, Some(performancePoint)))
      caches.toMap
        .filter { cache => preSnapshot.get(cache._1) != Some(cache._2) }
    }
  }


  /**
    * 更新一个cache的metrics
    */
  override def update(metrics: CacheState): Unit = {
    caches.update(metrics.tp, metrics)
  }

  private def noAssignableSpace = miniCacheSize * caches.size >= maxSize


  /**
    * 先从低评分的cache尝试获取空间
    * 会释放超过minSize的部分
    *
    * @param targetSize 想要释放的内存大小
    * @return 受到影响的TopicPartition在调整之后的大小
    */
  private def free(targetSize: Int, miniSize: Int): Map[TopicPartition, Int] = {
    val waitIterator = caches.values.toArray.sortBy(_.point.getOrElse(0.0)).iterator
    val effected = mutable.HashMap.empty[TopicPartition, Int]
    var freed = 0
    while (waitIterator.hasNext && freed < targetSize) {
      val cache = waitIterator.next()
      if (cache.currentSize <= miniSize) {
        //不需要修改这个cache的大小
        //do nothing
      } else {
        //此时已经肯定会修改这个cache
        val totalToFree = targetSize - freed
        val cacheFreeable = cache.currentSize - miniSize
        val cacheToFree = Math.min(totalToFree, cacheFreeable) //它肯定会大于0
        freed += cacheToFree
        val newCacheSize = cache.currentSize - cacheToFree
        effected.put(cache.tp, newCacheSize)
        caches.put(cache.tp, cache.copy(currentSize = newCacheSize, version = cache.version + 1))
      }
    }
    effected.toMap
  }


  /**
    * 对于每个cache，它超过miniSize的部分为可释放的部分
    * 在选择从其它cache索取空间时，首先所有其它cache超过miniSize的部分都可以被用来满足当前cache的miniSize的需求。
    * 在达到miniSize以后，其余的空间只会从performance point超过此base point的cache里抢夺
    *
    * @param targetSize 一共想要释放的内存大小
    * @param miniSize   每个cache的内存的最小值
    * @param basePoint  当已经释放够minSize的内存时，不会释放其point大于此值的cache所占据内存
    * @return 一共释放了多少内存
    */
  def tryFree(targetSize: Int, miniSize: Int, basePoint: Double): Int = {
    var freeable = caches.filter(_._2.currentSize > miniSize).values.toArray.sortBy(_.point.getOrElse(0.0))
    var freedSize = 0
    var freeableIter = freeable.iterator
    while (freedSize < miniSize && freedSize < targetSize && freeableIter.hasNext) {
      freedSize += free(Math.min(targetSize, miniSize) - freedSize, freeableIter.next(), miniSize)
    }
    if (freedSize < miniSize || freedSize >= targetSize) {
      freedSize
    } else {
      freeable = caches
        .filter(e => e._2.currentSize > miniSize && e._2.point.getOrElse(0.0) < basePoint)
        .values
        .toArray
        .sortBy(_.point)

      freeableIter = freeable.iterator
      while (freedSize < targetSize && freeableIter.hasNext) {
        freedSize += free(targetSize - freedSize, freeableIter.next(), miniSize)
      }
      freedSize
    }
  }

  /**
    * 从单个cache里释放指定大小的内存
    *
    * @param targetSize 目标大小
    * @param fromCache  从此cache占据内存释放
    * @param minSize    每个cache的最小值
    * @return 一共释放了多少内存
    */
  private def free(targetSize: Int, fromCache: CacheState, minSize: Int): Int = {
    val freeable = Math.max(0, fromCache.currentSize - minSize)
    val toFree = Math.min(freeable, targetSize)
    caches.put(
      fromCache.tp,
      fromCache.copy(
        version = fromCache.version + 1,
        currentSize = fromCache.currentSize - toFree)
    )
    toFree
  }

  override def getState(tp: TopicPartition): Option[CacheState] = caches.get(tp)

  override def currentCaches() = caches.toMap

  override def actorOf(tp: TopicPartition) = caches.get(tp).map(_.actor)

  override def getCaches(): List[CacheState] =
    caches.toMap.values.toList // has to be immutable
}

