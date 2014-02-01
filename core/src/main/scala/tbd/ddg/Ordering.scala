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
  var base: Timestamp = new Timestamp(0, null)
  base.next = base

  def after(t: Timestamp): Timestamp = {
    val previous =
      if (t == null) {
        base
      } else {
        t
      }
    val v0 = previous.time

    var j = 1
    var vj = previous.next
    var wj =
      if (vj == base) {
        maxSize
      } else {
        (vj.time - v0) % maxSize
      }
    while (wj <= j * j) {
      vj = vj.next
      j += 1
      wj =
        if (vj == base) {
          maxSize
        } else {
          (vj.time - v0) % maxSize
        }
    }

    var sx = previous.next
    for (i <- 1 to j - 1) {
      sx.time = (wj * (i / j) + v0) % maxSize
      sx = sx.next
    }

    val nextTime =
      if (previous.next == base) {
        maxSize
      } else {
        previous.next.time
      }

    val newTimestamp = new Timestamp((v0 + nextTime) / 2, previous.next)
    previous.next = newTimestamp

    newTimestamp
  }

  private def increment(t: Timestamp) {
    var stamp = t
    while (stamp != base) {
      stamp.time += 1
      stamp = stamp.next
    }
  }

  def remove(t: Timestamp) {
    var node = base
    var previous: Timestamp  = null
    while (node != t) {
      previous = node
      node = node.next
    }

    if (previous == null) {
      base = node.next
    } else {
      previous.next = node.next
    }

    decrement(node)
  }

  private def decrement(t: Timestamp) {
    var stamp = t
    while (stamp != base) {
      stamp.time -= 1
      stamp = stamp.next
    }
  }

  override def toString = {
    var node = base.next
    var ret = ""

    while (node != base) {
      ret += node + ", "
      node = node.next
    }
    ret
  }
}
