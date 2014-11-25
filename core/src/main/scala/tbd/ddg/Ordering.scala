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

import scala.collection.mutable.Buffer

import tbd.Constants._

class Ordering(basePointer: Pointer = -1) {
  private val maxSize = Int.MaxValue / 2
  val base = new Sublist(Sublist.create(0, -1), null)

  base.nextSub = new Sublist(Sublist.create(1, base.ptr), base, basePointer)
  Sublist.setNextSub(base.ptr, base.nextSub.ptr)
  base.previousSub = base.nextSub

  base.nextSub.base.end = base.base
  Timestamp.setEndPtr(base.nextSub.base.ptr, base.base.ptr)

  def after(ptr: Pointer, nodePtr: Pointer): Pointer = {
    val previousSublistPtr =
      if (ptr == -1) {
        Sublist.getNextSub(base.ptr)
      } else {
        Timestamp.getSublistPtr(ptr)
      }

    val newTimePtr = Sublist.after(previousSublistPtr, ptr, nodePtr)
    val newTimestamp = Timestamp.getTimestamp(newTimePtr)
    if (Sublist.getSize(previousSublistPtr) > 63) {
      val newSublist = sublistAfter(previousSublistPtr)
      Sublist.split(previousSublistPtr, newSublist.ptr)
    }

    newTimePtr
  }

  def append(ptr: Pointer): Pointer = {
    val newTimePtr =
      if (Sublist.getSize(base.previousSub.ptr) > 31) {
        val newSublist = sublistAppend()
        Sublist.append(newSublist.ptr, ptr)
      } else {
        Sublist.append(base.previousSub.ptr, ptr)
      }

    newTimePtr
  }

  def remove(ptr: Pointer) {
    val t = Timestamp.getTimestamp(ptr)
    Sublist.remove(t.sublist.ptr, t.ptr)

    if (Sublist.getSize(t.sublist.ptr) == 0) {
      t.sublist.previousSub.nextSub = t.sublist.nextSub
      Sublist.setNextSub(t.sublist.previousSub.ptr, t.sublist.nextSub.ptr)

      t.sublist.nextSub.previousSub = t.sublist.previousSub
    }
  }

  private def sublistAppend(): Sublist = {
    val previous = base.previousSub

    val newId = Sublist.getId(previous.ptr) + 1
    val newSublist = new Sublist(Sublist.create(newId, base.ptr), base)
    newSublist.previousSub = previous

    previous.nextSub = newSublist
    Sublist.setNextSub(previous.ptr, newSublist.ptr)

    base.previousSub = newSublist

    newSublist
  }

  private def sublistAfter(_s: Pointer): Sublist = {
    val s = Sublist.getSublist(_s)

    var node = s.nextSub
    while (node != base) {
      Sublist.setId(node.ptr, Sublist.getId(node.ptr) + 1)
      node = node.nextSub
    }

    val newId = Sublist.getId(s.ptr) + 1
    val newSublist = new Sublist(Sublist.create(newId, s.nextSub.ptr), s.nextSub)
    newSublist.previousSub = s

    s.nextSub = newSublist
    Sublist.setNextSub(s.ptr, newSublist.ptr)
    newSublist.nextSub.previousSub = newSublist

    newSublist
    /*val previous =
      if (s == null) {
        base
      } else {
        s
      }
    val v0 = previous.id

    var j = 1
    var vj = previous.next
    var wj =
      if (vj == base) {
        maxSize
      } else {
        (vj.id - v0) % maxSize
      }
    while (wj <= j * j) {
      vj = vj.next
      j += 1
      wj =
        if (vj == base) {
          maxSize
        } else {
          (vj.id - v0) % maxSize
        }
    }

    var sx = previous.next
    for (i <- 1 to j - 1) {
      sx.id = (wj * (i / j) + v0) % maxSize
      sx = sx.next
    }

    val nextId =
      if (previous.next == base) {
        maxSize
      } else {
        previous.next.id
      }

    val newSublist = new Sublist((v0 + nextId) / 2, previous.next)
    previous.next = newSublist

    newSublist*/
  }

  override def toString = {
    var node = base.nextSub
    var ret = base.toString

    while (node != base) {
      print(node + " ")
      ret += ", " + node
      node = node.nextSub
    }
    ret
  }
}

