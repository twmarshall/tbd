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
  val basePtr = Sublist.create(0, -1)

  private val nextSubPtr = Sublist.create(1, basePtr, basePointer)
  Sublist.setNextSub(basePtr, nextSubPtr)
  Sublist.setPreviousSub(basePtr, nextSubPtr)

  Timestamp.setEndPtr(
    Sublist.getBasePtr(nextSubPtr),
    Sublist.getBasePtr(basePtr))

  def after(ptr: Pointer, nodePtr: Pointer): Pointer = {
    val previousSublistPtr =
      if (ptr == -1) {
        Sublist.getNextSub(basePtr)
      } else {
        Timestamp.getSublistPtr(ptr)
      }

    val newTimePtr = Sublist.after(previousSublistPtr, ptr, nodePtr)
    if (Sublist.getSize(previousSublistPtr) > 63) {
      val newSublistPtr = sublistAfter(previousSublistPtr)
      Sublist.split(previousSublistPtr, newSublistPtr)
    }

    newTimePtr
  }

  def append(ptr: Pointer): Pointer = {
    val basePreviousSubPtr = Sublist.getPreviousSub(basePtr)
    val newTimePtr =
      if (Sublist.getSize(basePreviousSubPtr) > 31) {
        val newSublistPtr = sublistAppend()
        Sublist.append(newSublistPtr, ptr)
      } else {
        Sublist.append(basePreviousSubPtr, ptr)
      }

    newTimePtr
  }

  def remove(ptr: Pointer) {
    val sublistPtr = Timestamp.getSublistPtr(ptr)
    Sublist.remove(sublistPtr, ptr)

    if (Sublist.getSize(sublistPtr) == 0) {
      val nextSubPtr = Sublist.getNextSub(sublistPtr)
      val previousSubPtr = Sublist.getPreviousSub(sublistPtr)
      Sublist.setNextSub(previousSubPtr, nextSubPtr)
      Sublist.setPreviousSub(nextSubPtr, previousSubPtr)
    }
  }

  private def sublistAppend(): Pointer = {
    val basePreviousSubPtr = Sublist.getPreviousSub(basePtr)

    val newId = Sublist.getId(basePreviousSubPtr) + 1
    val newSublistPtr = Sublist.create(newId, basePtr)
    Sublist.setPreviousSub(newSublistPtr, basePreviousSubPtr)

    Sublist.setNextSub(basePreviousSubPtr, newSublistPtr)
    Sublist.setPreviousSub(basePtr, newSublistPtr)

    newSublistPtr
  }

  private def sublistAfter(subPtr: Pointer): Pointer = {
    var nodePtr = Sublist.getNextSub(subPtr)
    while (nodePtr != basePtr) {
      Sublist.setId(nodePtr, Sublist.getId(nodePtr) + 1)
      nodePtr = Sublist.getNextSub(nodePtr)
    }

    val newId = Sublist.getId(subPtr) + 1
    val nextSubPtr = Sublist.getNextSub(subPtr)
    val newSublistPtr = Sublist.create(newId, nextSubPtr)

    Sublist.setNextSub(subPtr, newSublistPtr)
    Sublist.setPreviousSub(nextSubPtr, newSublistPtr)

    newSublistPtr
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
    var subPtr = Sublist.getNextSub(basePtr)
    var ret = Sublist.toString(basePtr)

    while (subPtr != basePtr) {
      ret += ", " + Sublist.toString(subPtr)
      subPtr = Sublist.getNextSub(subPtr)
    }
    ret
  }
}

