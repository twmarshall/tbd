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
package tbd.ddg

import tbd.MemoryAllocator
import tbd.Constants._

object Timestamp {
  private val timestamps = scala.collection.mutable.Map[Pointer, Timestamp]()
  private val lock = new java.util.concurrent.locks.ReentrantLock()

  def getTimestamp(ptr: Pointer): Timestamp = {
    lock.lock()
    val t = timestamps(ptr)
    lock.unlock()
    t
  }

  private val sublistPtrOffset = 0
  private val timeOffset = sublistPtrOffset + pointerSize
  private val nextTimeOffset = timeOffset + 8
  private val previousTimeOffset = nextTimeOffset + pointerSize
  private val nodePtrOffset = previousTimeOffset + pointerSize
  private val endPtrOffset = nodePtrOffset + pointerSize

  def create
    (sublistPtr: Pointer,
     time: Double,
     nextTime: Pointer,
     previousTime: Pointer,
     nodePtr: Pointer,
     timestamp: Timestamp): Pointer = {
    val size = pointerSize * 5 + 8
    val ptr = MemoryAllocator.allocate(size)

    MemoryAllocator.putPointer(ptr, sublistPtr)
    MemoryAllocator.putDouble(ptr + timeOffset, time)
    MemoryAllocator.putPointer(ptr + nextTimeOffset, nextTime)
    MemoryAllocator.putPointer(ptr + previousTimeOffset, previousTime)
    MemoryAllocator.putPointer(ptr + nodePtrOffset, nodePtr)
    MemoryAllocator.putPointer(ptr + endPtrOffset, -1)

    lock.lock()
    timestamps(ptr) = timestamp
    lock.unlock()

    ptr
  }

  def getSublistPtr(ptr: Pointer): Pointer = {
    MemoryAllocator.getPointer(ptr + sublistPtrOffset)
  }

  def setSublistPtr(ptr: Pointer, newSublistPtr: Pointer) {
    MemoryAllocator.putPointer(ptr + sublistPtrOffset, newSublistPtr)
  }

  def getTime(ptr: Pointer): Double = {
    MemoryAllocator.getDouble(ptr + timeOffset)
  }

  def setTime(ptr: Pointer, newTime: Double) {
    MemoryAllocator.putDouble(ptr + timeOffset, newTime)
  }

  def getNextTime(ptr: Pointer): Pointer = {
    MemoryAllocator.getPointer(ptr + nextTimeOffset)
  }

  def setNextTime(ptr: Pointer, newNextTime: Pointer) {
    MemoryAllocator.putPointer(ptr + nextTimeOffset, newNextTime)
  }

  def getPreviousTime(ptr: Pointer): Pointer = {
    MemoryAllocator.getPointer(ptr + previousTimeOffset)
  }

  def setPreviousTime(ptr: Pointer, newPreviousTime: Pointer) {
    MemoryAllocator.putPointer(ptr + previousTimeOffset, newPreviousTime)
  }

  def getNodePtr(ptr: Pointer): Pointer = {
    MemoryAllocator.getPointer(ptr + nodePtrOffset)
  }

  def getEndPtr(ptr: Pointer): Pointer = {
    MemoryAllocator.getPointer(ptr + endPtrOffset)
  }

  def setEndPtr(ptr: Pointer, newEndPtr: Pointer) {
    MemoryAllocator.putPointer(ptr + endPtrOffset, newEndPtr)
  }

  def getNext(thisPtr: Pointer): Pointer = {
    val nextPtr = getNextTime(thisPtr)
    val sublistPtr = getSublistPtr(thisPtr)
    val sublistBasePtr = Sublist.getBasePtr(sublistPtr)

    if (nextPtr != sublistBasePtr) {
      nextPtr
    } else {
      val nextSublistPtr = Sublist.getNextSub(sublistPtr)
      val nextBasePtr = Sublist.getBasePtr(nextSublistPtr)
      getNext(nextBasePtr)
    }
  }

  def toString(ptr: Pointer): String = {
    "Timestamp " + getTime(ptr)
  }

  def <(ptr1: Pointer, ptr2: Pointer): Boolean = {
    val sublistPtr1 = getSublistPtr(ptr1)
    val sublistPtr2 = getSublistPtr(ptr2)

    if (sublistPtr1 == sublistPtr2) {
      val time1 = getTime(ptr1)
      val time2 = getTime(ptr2)

      time1 < time2
    } else {
      Sublist.getId(sublistPtr1) < Sublist.getId(sublistPtr2)
    }
  }

  def >(ptr1: Pointer, ptr2: Pointer): Boolean = {
    val sublistPtr1 = getSublistPtr(ptr1)
    val sublistPtr2 = getSublistPtr(ptr2)

    if (sublistPtr1 == sublistPtr2) {
      val time1 = getTime(ptr1)
      val time2 = getTime(ptr2)

      time1 > time2
    } else {
      Sublist.getId(sublistPtr1) > Sublist.getId(sublistPtr2)
    }
  }

  def >=(ptr1: Pointer, ptr2: Pointer): Boolean = {
    !(<(ptr1, ptr2))
  }

  private val maxSublist = new Sublist(Sublist.create(Int.MaxValue, -1), null)

  // A dummy timestamp which all real Timestamps are less than. Only use for
  // comparison since it isn't actually attached to the ordering data structure.
  val MAX_TIMESTAMP = new Timestamp(maxSublist, null, null)
  MAX_TIMESTAMP.ptr = Timestamp.create(maxSublist.ptr, Int.MaxValue, -1, -1, -1, MAX_TIMESTAMP)

  private val minSublist = new Sublist(Sublist.create(-1, -1), null)

  // A dummy timestamp which all real Timestamps are greater than.
  val MIN_TIMESTAMP = new Timestamp(minSublist, null, null)
  MIN_TIMESTAMP.ptr = Timestamp.create(minSublist.ptr, -1, -1, -1, -1, MIN_TIMESTAMP)
}

class Timestamp
    (var sublist: Sublist,
     var next: Timestamp,
     var previous: Timestamp) {
  var ptr: Long = -1

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[Timestamp]) {
      false
    } else {
      val that = obj.asInstanceOf[Timestamp]

      Timestamp.getTime(that.ptr) == Timestamp.getTime(ptr) &&
      Sublist.getId(Timestamp.getSublistPtr(that.ptr)) ==
      Sublist.getId(Timestamp.getSublistPtr(ptr))
    }
  }

  override def toString =
    "(" + Sublist.getId(sublist.ptr) + " " + Timestamp.getTime(ptr) + ")"
}

class TimestampOrdering extends scala.math.Ordering[Timestamp] {
  def compare(one: Timestamp, two: Timestamp) = {
    if (Timestamp.>(one.ptr, two.ptr)) {
      1
    } else if (one == two) {
      0
    } else {
      -1
    }
  }
}
