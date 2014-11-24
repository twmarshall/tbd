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
import tbd.Constants.Pointer

object Sublist {
  val idOffset = 0
  val nextOffset = idOffset + 4

  def create(id: Int, next: Pointer): Pointer = {
    val size = 4 + 8
    val ptr = MemoryAllocator.allocate(size)

    MemoryAllocator.putInt(ptr + idOffset, id)
    MemoryAllocator.putPointer(ptr + nextOffset, next)

    ptr
  }

  def getId(ptr: Pointer): Int = {
    MemoryAllocator.getInt(ptr + idOffset)
  }

  def setId(ptr: Pointer, newId: Int) {
    MemoryAllocator.putInt(ptr + idOffset, newId)
  }
}

class Sublist
    (id: Int,
     var nextSub: Sublist,
     var nextPointer: Pointer,
     basePointer: Pointer = -1) {

  val ptr = Sublist.create(id, 0)

  var previous: Sublist = null

  val base: Timestamp = new Timestamp(this, ptr, 0, null, null, basePointer)

  base.next = base
  base.previous = base

  var size = 0

  def after(t: Timestamp, nodePtr: Pointer): Timestamp = {
    val previous =
      if (t == null) {
        base
      } else {
        t
      }

    val newTimestamp =
      if (previous.next == base) {
        new Timestamp(this, ptr, previous.time + 1, base, previous, nodePtr)
      } else {
        new Timestamp(
          this,
          ptr,
          (previous.time + previous.next.time) / 2,
          previous.next,
          previous,
          nodePtr)
      }

    previous.next = newTimestamp
    newTimestamp.next.previous = newTimestamp
    size += 1

    newTimestamp
  }

  def append(nodePtr: Pointer): Timestamp = {
    val previous = base.previous
    val newTimestamp =
      new Timestamp(this, ptr, previous.time + 1, base, previous, nodePtr)

    previous.next = newTimestamp
    newTimestamp.next.previous = newTimestamp
    size += 1

    newTimestamp
  }

  def remove(t: Timestamp) {
    t.previous.next = t.next
    t.next.previous = t.previous
    size -= 1
  }

  def split(newSublist: Sublist) {
    var node = base
    var i = 0
    while (i < 32) {
      node = node.next
      node.time = i + 1
      i += 1
    }
    val newStart = node.next
    newSublist.base.next = newStart
    newStart.previous = newSublist.base

    node.next = base
    base.previous = node
    this.size = i

    node = newSublist.base
    i = 0
    while (node.next != base) {
      node = node.next
      node.time = i + 1

      node.sublist = newSublist
      node.sublistPtr = node.sublist.ptr

      i += 1
    }

    newSublist.base.previous = node
    node.next = newSublist.base
    newSublist.size = i
  }

  override def toString = {
    var node = base.next
    var ret = "(" + size + ") {"

    while (node != base) {
      ret += node + ", "
      node = node.next
    }
    ret + "}"
  }
}
