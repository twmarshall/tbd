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

class OrderingNode(aTimestamp: Timestamp, aNext: OrderingNode) {
  val timestamp = aTimestamp
  var next = aNext
}

class Ordering {
  var timestamps: OrderingNode = null

  def after(t: Timestamp): Timestamp = {
    var node = timestamps
    var newTimestamp: Timestamp = null
    if (t == null) {
      // Insert at head.
      newTimestamp = new Timestamp(0)
      timestamps = new OrderingNode(newTimestamp, timestamps)
    } else {
      newTimestamp = new Timestamp(t.time + 1)

      while (node.timestamp != t) {
	node = node.next
      }

      node.next = new OrderingNode(newTimestamp, node.next)
      node = node.next.next
    }

    // Increment all of the timestamps after the inserted one.
    while (node != null) {
      node.timestamp.increment()
      node = node.next
    }

    newTimestamp
  }

  def remove(t: Timestamp) {
    var node = timestamps
    var previous: OrderingNode = null
    while (node.timestamp != t) {
      previous = node
      node = node.next
    }

    if (previous == null) {
      // Removing head.
      timestamps = timestamps.next
    } else {
      previous.next = node.next
    }
  }

  override def toString = {
    var node = timestamps
    var ret = ""
    while (node != null) {
      ret += node.timestamp + ", "
      node = node.next
    }
    ret
  }
}
