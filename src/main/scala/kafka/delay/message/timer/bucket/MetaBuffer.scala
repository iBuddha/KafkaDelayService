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

import java.nio.ByteBuffer

import kafka.delay.message.timer.meta.DelayMessageMeta


/**
  * An append-only, non-threadsafe, array-backed vector that is optimized for primitive types.
  * 用于存储DelayMessageMeta
  * 它底层使用一个byte array来存储数据，需要尽量压缩数据以减少在内存中占据的大小
  */
class MetaBuffer(initialElements: Int = 64)  extends MetaBufferLike {
  private var _numElements = 0
  private var _capacity = initialElements
  private var _array: Array[Byte] = _
  private var _byteBuffer: ByteBuffer = _ //它wrap了_array
  private val _elementSizeInByte = DelayMessageMeta.SizeInByte

  // NB: This must be separate from the declaration, otherwise the specialized parent class
  // will get its own array with the same initial size.
  _array = new Array[Byte](initialElements * _elementSizeInByte)
  _byteBuffer = ByteBuffer.wrap(_array)

  def +=(meta: DelayMessageMeta): Unit = {
    if(_numElements == capacity){
      resize((capacity * 1.5).toInt)
    }
    assert(_numElements < capacity)
    DelayMessageMeta.writeTo(meta, _byteBuffer)
    _numElements += 1
  }

  def capacity: Int = _capacity

  def sizeInByte = _capacity * _elementSizeInByte

  //count of metadata
  override def size: Int = _numElements

  def iterator: Iterator[DelayMessageMeta] = new Iterator[DelayMessageMeta] {
    val buffer = _byteBuffer.duplicate()
    var index = 0
    val numberElement = _numElements
    buffer.position(0)
    buffer.limit(_numElements * _elementSizeInByte)

    override def hasNext: Boolean = index < numberElement

    override def next(): DelayMessageMeta = {
      if (!hasNext) {
        throw new NoSuchElementException
      }
      val value = DelayMessageMeta.readFrom(buffer)
      index += 1
      value
    }
  }

//  /** Gets the underlying array backing this vector. */
//  private def array: Array[Byte] = _array

  /** Trims this vector so that the capacity is equal to the size. */
  def trim(): MetaBuffer = resize(size)

  def reset(): Unit = {
    val newSize = Math.max(_numElements / 2, initialElements)
    _array = new Array[Byte](newSize * _elementSizeInByte)
    _byteBuffer = ByteBuffer.wrap(_array)
    _capacity = newSize
    _numElements = 0
  }

  /** Resizes the array, dropping elements if the total length decreases. */
  private def resize(newSize: Int): MetaBuffer = {
    val newLength = newSize * _elementSizeInByte
    val oldPosition = _byteBuffer.position()
    _array = copyArrayWithLength(newLength)
    _byteBuffer = ByteBuffer.wrap(_array)
    _byteBuffer.position(oldPosition)
    _capacity = newSize
    this
  }

  /** Return a trimmed version of the underlying array. */
//  private def toArray: Array[Byte] = {
//    copyArrayWithLength(size * ELEMENT_SIZE)
//  }

  /**
    * copy current array to a new array with size of length
    *
    * @param length in bytes
    * @return
    */
  private def copyArrayWithLength(length: Int): Array[Byte] = {
    val copy = new Array[Byte](length)
    _array.copyToArray(copy)
    copy
  }
}
