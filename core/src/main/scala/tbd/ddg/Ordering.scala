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
  var start: Timestamp = null

  def after(t: Timestamp): Timestamp = {
    if (t == null) {
      increment(start)
      val newTimestamp = new Timestamp(0, start)
      start = newTimestamp

      newTimestamp
    } else {
      val newTimestamp = new Timestamp(t.time + 1, t.next)
      t.next = newTimestamp
      increment(newTimestamp.next)

      newTimestamp
    }
  }

  private def increment(t: Timestamp) {
    var stamp = t
    while (stamp != null) {
      stamp.time += 1
      stamp = stamp.next
    }
  }

  def remove(t: Timestamp) {
    var node = start
    var previous: Timestamp  = null
    while (node != t) {
      previous = node
      node = node.next
    }

    if (previous == null) {
      start = node.next
    } else {
      previous.next = node.next
    }

    decrement(node)
  }

  private def decrement(t: Timestamp) {
    var stamp = t
    while (stamp != null) {
      stamp.time -= 1
      stamp = stamp.next
    }
  }

  override def toString = {
    var node = start
    var ret = ""

    while (node != null) {
      ret += node + ", "
      node = node.next
    }
    ret
  }
}
