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

object Sublist {
  val idOffset = 0
  val sizeOffset = idOffset + 4
  val nextSubOffset = sizeOffset + 4
  val previousSubOffset = nextSubOffset + pointerSize
  val baseOffset = previousSubOffset + pointerSize

  def create(id: Int, nextSub: Pointer): Pointer = {
    val size = 4 * 2 + pointerSize * 3
    val ptr = MemoryAllocator.allocate(size)

    MemoryAllocator.putInt(ptr + idOffset, id)
    MemoryAllocator.putInt(ptr + sizeOffset, 0)
    MemoryAllocator.putPointer(ptr + nextSubOffset, nextSub)
    MemoryAllocator.putPointer(ptr + previousSubOffset, -1)
    MemoryAllocator.putPointer(ptr + baseOffset, -1)

    ptr
  }

  def getId(ptr: Pointer): Int = {
    MemoryAllocator.getInt(ptr + idOffset)
  }

  def setId(ptr: Pointer, newId: Int) {
    MemoryAllocator.putInt(ptr + idOffset, newId)
  }

  def getSize(ptr: Pointer): Int = {
    MemoryAllocator.getInt(ptr + sizeOffset)
  }

  def setSize(ptr: Pointer, newSize: Int) {
    MemoryAllocator.putInt(ptr + sizeOffset, newSize)
  }

  def getNextSub(ptr: Pointer): Pointer = {
    MemoryAllocator.getPointer(ptr + nextSubOffset)
  }

  def setNextSub(ptr: Pointer, newNextSub: Pointer) {
    MemoryAllocator.putPointer(ptr + nextSubOffset, newNextSub)
  }

  def getBasePtr(thisPtr: Pointer): Pointer = {
    MemoryAllocator.getPointer(thisPtr + baseOffset)
  }

  def setBasePtr(thisPtr: Pointer, newBasePtr: Pointer) {
    MemoryAllocator.putPointer(thisPtr + baseOffset, newBasePtr)
  }

  def calculateSize(ptr: Pointer): Int = {
    var timePtr = Timestamp.getNextTime(ptr)

    var size = 0
    while (timePtr != ptr) {
      size += 1
      timePtr = Timestamp.getNextTime(timePtr)
    }

    size
  }
}

class Sublist
    (val ptr: Long,
     var nextSub: Sublist,
     basePointer: Pointer = -1) {

  var previousSub: Sublist = null

  val base: Timestamp = new Timestamp(this, null, null)
  base.ptr = Timestamp.create(ptr, 0, -1, -1, basePointer, base)
  val basePtr = base.ptr
  Sublist.setBasePtr(ptr, basePtr)

  base.next = base
  Timestamp.setNextTime(base.ptr, base.ptr)
  base.previous = base
  Timestamp.setPreviousTime(base.ptr, base.ptr)

  def after(t: Timestamp, nodePtr: Pointer): Timestamp = {
    val previous =
      if (t == null) {
        base
      } else {
        t
      }

    val newTimestamp =
      if (Timestamp.getNextTime(previous.ptr) == base.ptr) {
        val time = new Timestamp(
          this,
          base,
          previous)
        time.ptr = Timestamp.create(ptr, Timestamp.getTime(previous.ptr) + 1,
                                    base.ptr, previous.ptr, nodePtr, time)
        time
      } else {
        val nextTimePtr = Timestamp.getNextTime(previous.ptr)
        val newTime = (Timestamp.getTime(previous.ptr) +
                       Timestamp.getTime(nextTimePtr)) / 2
        val time = new Timestamp(
          this,
          previous.next,
          previous)
        time.ptr = Timestamp.create(ptr, newTime, nextTimePtr, previous.ptr, nodePtr, time)
        time
      }

    previous.next = newTimestamp
    Timestamp.setNextTime(previous.ptr, newTimestamp.ptr)
    newTimestamp.next.previous = newTimestamp
    Timestamp.setPreviousTime(Timestamp.getNextTime(newTimestamp.ptr), newTimestamp.ptr)
    Sublist.setSize(ptr, Sublist.getSize(ptr) + 1)

    newTimestamp
  }

  def append(nodePtr: Pointer): Timestamp = {
    val previous = base.previous
    val previousPtr = Timestamp.getPreviousTime(base.ptr)
    val newTimestamp =
      new Timestamp(
        this,
        base,
        previous)
    newTimestamp.ptr = Timestamp.create(ptr,
      Timestamp.getTime(previousPtr) + 1, base.ptr, previousPtr, nodePtr, newTimestamp)

    previous.next = newTimestamp
    Timestamp.setNextTime(previousPtr, newTimestamp.ptr)
    newTimestamp.next.previous = newTimestamp
    Timestamp.setPreviousTime(Timestamp.getNextTime(newTimestamp.ptr), newTimestamp.ptr)
    Sublist.setSize(ptr, Sublist.getSize(ptr) + 1)

    newTimestamp
  }

  def remove(t: Timestamp) {
    val previousPtr = Timestamp.getPreviousTime(t.ptr)
    val nextPtr = Timestamp.getNextTime(t.ptr)

    t.previous.next = t.next
    Timestamp.setNextTime(previousPtr, nextPtr)
    t.next.previous = t.previous
    Timestamp.setPreviousTime(nextPtr, previousPtr)
    Sublist.setSize(ptr, Sublist.getSize(ptr) - 1)
  }

  def split(newSublist: Sublist) {
    var node = base
    var nodePtr = base.ptr
    var i = 0
    while (i < 32) {
      node = node.next
      nodePtr = Timestamp.getNextTime(nodePtr)
      Timestamp.setTime(nodePtr, i + 1)
      i += 1
    }
    val newStart = node.next
    val newStartPtr = Timestamp.getNextTime(nodePtr)
    newSublist.base.next = newStart
    Timestamp.setNextTime(newSublist.base.ptr, newStartPtr)
    newStart.previous = newSublist.base
    Timestamp.setPreviousTime(newStartPtr, newSublist.base.ptr)

    node.next = base
    Timestamp.setNextTime(nodePtr, base.ptr)
    base.previous = node
    Timestamp.setPreviousTime(base.ptr, node.ptr)
    Sublist.setSize(ptr, i)

    node = newSublist.base
    nodePtr = newSublist.base.ptr
    i = 0
    while (Timestamp.getNextTime(nodePtr) != basePtr) {
      node = node.next
      nodePtr = Timestamp.getNextTime(nodePtr)
      Timestamp.setTime(nodePtr, i + 1)

      node.sublist = newSublist
      Timestamp.setSublistPtr(nodePtr, node.sublist.ptr)

      i += 1
    }

    newSublist.base.previous = node
    Timestamp.setPreviousTime(newSublist.base.ptr, node.ptr)
    node.next = newSublist.base
    Timestamp.setNextTime(node.ptr, newSublist.base.ptr)
    Sublist.setSize(newSublist.ptr, i)
  }

  override def toString = {
    var nodePtr = Timestamp.getNextTime(base.ptr)
    var ret = "(" + Sublist.getSize(ptr) + ") {"

    while (nodePtr != basePtr) {
      ret += Timestamp.toString(nodePtr) + ", "
      nodePtr = Timestamp.getNextTime(nodePtr)
    }
    ret + "}"
  }
}
