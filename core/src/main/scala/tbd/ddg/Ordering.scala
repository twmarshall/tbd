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

class Ordering {
  val maxSize = Int.MaxValue / 2
  var base = new Sublist(0, null)
  base.next = new Sublist(1, base)
  base.previous = base.next

  def after(t: Timestamp): Timestamp = {
    val previousSublist =
      if (t == null) {
        base.next
      } else {
        t.sublist
      }

    val newTimestamp = previousSublist.after(t)
    if (previousSublist.size > 63) {
      val newSublist = sublistAfter(previousSublist)
      assert(previousSublist.id != newSublist.id)
      previousSublist.split(newSublist)
    }

    newTimestamp
  }

  private def sublistAfter(s: Sublist): Sublist = {
    var node = s.next
    while (node != base) {
      node.id += 1
      node = node.next
    }
    val newSublist = new Sublist(s.id + 1, s.next)
    s.next = newSublist
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

  def remove(t: Timestamp) {
    t.sublist.remove(t)
  }

  override def toString = {
    var node = base.next
    var ret = base.toString

    while (node != base) {
      ret += ", " + node
      node = node.next
    }
    ret
  }
}

