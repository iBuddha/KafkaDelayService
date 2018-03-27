/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package kafka.delay.message.timer.bucket

import kafka.delay.message.timer.meta.DelayMessageMeta

import scala.collection.mutable


/**
  * 用于存储DelayMessageMeta
  * 它的存储分成两部分：
  * stashed: 暂存的消息
  * compressed: 压缩后的消息
  * 当stashed的数量超过给定值，就会对它们进行压缩
  *
  *
  * 最多只支持存放24天以内的延迟, 以及offset的差值在Int.MaxValue之内的消息
  *
  * @param initialElements  初始大小
  * @param compressUnitSize 当暂存的数据达到此数量后，对暂存的数据进行压缩
  *
  *
  */
class CompactMetaBuffer(initialElements: Int = 64, compressUnitSize: Int = 16384) extends MetaBufferLike {
  private val stashed: MetaBuffer = new MetaBuffer(initialElements)
  private val compressed: mutable.MutableList[CompactedMetaArray] = mutable.MutableList.empty

  def sizeInByte = stashed.size * 2 * 4 + compressed.map(_.sizeInByte).sum

  override def +=(meta: DelayMessageMeta): Unit = {
    if (stashed.size == compressUnitSize) {
      val toCompress = stashed.iterator.toArray
      val compressedUnit = CompactedMetaArray(toCompress)
      compressed += compressedUnit
      stashed.reset()
      +=(meta)
    } else {
      stashed += meta
    }
  }

  //count of metadata
  override def size: Int = stashed.size + compressed.map(_.length).sum

  override def iterator: Iterator[DelayMessageMeta] = new Iterator[DelayMessageMeta] {
    var compressedUnitIterator = compressed.iterator
    var compressedMetaIterator: Iterator[DelayMessageMeta] = stashed.iterator

    override def hasNext: Boolean = {
      if (compressedMetaIterator.hasNext) {
        true
      } else if (compressedUnitIterator.hasNext) {
        compressedMetaIterator = compressedUnitIterator.next.iterator
        hasNext
      } else {
        false
      }
    }

    override def next(): DelayMessageMeta = {
      compressedMetaIterator.next
    }
  }

  def reset(): Unit = {
    stashed.reset()
    compressed.clear()
  }
}
