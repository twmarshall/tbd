/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbd.util

import scala.collection.mutable.Buffer

import tbd.MemoryAllocator
import tbd.Constants._

object OffHeapMap {
  val sizeOffset = 0
  val numBucketsOffset = sizeOffset + 4
  val pointersOffset = numBucketsOffset + 4

  def create(numBuckets: Int = 1000): Pointer = {
    val size = 4 + numBuckets * pointerSize
    val ptr = MemoryAllocator.allocate(size)

    MemoryAllocator.putInt(ptr + sizeOffset, 0)
    MemoryAllocator.putInt(ptr + numBucketsOffset, numBuckets)

    for (i <- 0 until numBuckets * pointerSize) {
      MemoryAllocator.putPointer(ptr + pointersOffset + i, 0)
    }

    ptr
  }

  def getSize(thisPtr: Pointer): Int = {
    MemoryAllocator.getInt(thisPtr + sizeOffset)
  }

  def setSize(thisPtr: Pointer, newSize: Int) {
    MemoryAllocator.putInt(thisPtr + sizeOffset, newSize)
  }

  def getNumBuckets(thisPtr: Pointer): Int = {
    MemoryAllocator.getInt(thisPtr + numBucketsOffset)
  }

  def get(thisPtr: Pointer, key: String): Int = {
    val binPtr = getBinPtr(thisPtr, key)
    var nodePtr = MemoryAllocator.getPointer(binPtr)

    while (nodePtr != 0 && MapNode.getKey(nodePtr) != key) {
      nodePtr = MapNode.getNextNode(nodePtr)
    }

    MapNode.getValue(nodePtr)
  }

  def increment(thisPtr: Pointer, key: String, inc: Int) {
    val binPtr = getBinPtr(thisPtr, key)

    var previousPtr = binPtr
    var nodePtr = MemoryAllocator.getPointer(binPtr)
    while (nodePtr != 0 && MapNode.getKey(nodePtr) != key) {
      previousPtr = nodePtr
      nodePtr = MapNode.getNextNode(nodePtr)
    }

    if (nodePtr == 0) {
      val newNodePtr = MapNode.create(key, inc)

      MapNode.setNextNode(previousPtr, newNodePtr)
      setSize(thisPtr, getSize(thisPtr) + 1)
    } else {
      MapNode.increment(nodePtr, inc)
    }
  }

  def merge(thisPtr: Pointer, thatPtr: Pointer): Pointer = {
    val newPtr = create(getSize(thisPtr) + getSize(thatPtr))

    val thisSize = getNumBuckets(thisPtr)
    for (i <- 0 until thisSize) {
      val binPtr = thisPtr + pointersOffset + i * pointerSize
      var nodePtr = MemoryAllocator.getPointer(binPtr)

      while (nodePtr != 0) {
        val key = MapNode.getKey(nodePtr)
        val value = MapNode.getValue(nodePtr)
        increment(newPtr, key, value)

        nodePtr = MapNode.getNextNode(nodePtr)
      }
    }

    val thatSize = getNumBuckets(thatPtr)
    for (i <- 0 until thatSize) {
      val binPtr = thatPtr + pointersOffset + i * pointerSize
      var nodePtr = MemoryAllocator.getPointer(binPtr)

      while (nodePtr != 0) {
        val key = MapNode.getKey(nodePtr)
        val value = MapNode.getValue(nodePtr)
        increment(newPtr, key, value)

        nodePtr = MapNode.getNextNode(nodePtr)
      }
    }

    newPtr
  }

  def toBuffer(thisPtr: Pointer): Buffer[(String, Int)] = {
    val buf = Buffer[(String, Int)]()

    val thisSize = getNumBuckets(thisPtr)
    for (i <- 0 until thisSize) {
      val binPtr = thisPtr + pointersOffset + i * pointerSize
      var nodePtr = MemoryAllocator.getPointer(binPtr)

      while (nodePtr != 0) {
        val key = MapNode.getKey(nodePtr)
        val value = MapNode.getValue(nodePtr)
        buf += ((key, value))

        nodePtr = MapNode.getNextNode(nodePtr)
      }
    }

    buf
  }

  private def getBinPtr(thisPtr: Pointer, key: String): Pointer = {
    val size = getNumBuckets(thisPtr)
    val hash = key.hashCode().abs % size

    thisPtr + pointersOffset + hash * pointerSize
  }
}

object MapNode {
  val nextNodeOffset = 0
  val valueOffset = nextNodeOffset + pointerSize
  val keySizeOffset = valueOffset + 4
  val keyOffset = keySizeOffset + 4

  def create(key: String, value: Int): Pointer = {
    val ptr = MemoryAllocator.allocate(pointerSize + 4 * 2 + key.size * 2)

    MemoryAllocator.putPointer(ptr + nextNodeOffset, 0)
    MemoryAllocator.putInt(ptr + valueOffset, value)
    MemoryAllocator.putInt(ptr + keySizeOffset, key.size)

    for (i <- 0 until key.size) {
      MemoryAllocator.putChar(ptr + keyOffset + i * 2, key(i))
    }

    ptr
  }

  def getNextNode(thisPtr: Pointer): Pointer = {
    MemoryAllocator.getPointer(thisPtr + nextNodeOffset)
  }

  def setNextNode(thisPtr: Pointer, newNextNode: Pointer) {
    MemoryAllocator.putPointer(thisPtr, newNextNode)
  }

  def getKeySize(thisPtr: Pointer): Int = {
    MemoryAllocator.getInt(thisPtr)
  }

  def getKey(thisPtr: Pointer): String = {
    val keySize = MemoryAllocator.getInt(thisPtr + keySizeOffset)

    val arr = new Array[Char](keySize)
    for (i <- 0 until keySize) {
      arr(i) = MemoryAllocator.getChar(thisPtr + keyOffset + i * 2)
    }

    new String(arr)
  }

  def getValue(thisPtr: Pointer): Int = {
    MemoryAllocator.getInt(thisPtr + valueOffset)
  }

  def increment(thisPtr: Pointer, inc: Int) {
    val oldValue = getValue(thisPtr)
    MemoryAllocator.putInt(thisPtr + valueOffset, oldValue + inc)
  }
}
