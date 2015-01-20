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
package thomasdb.ddg

import scala.collection.mutable.Buffer

import thomasdb.Constants.ModId

class Ordering {
  val maxSize = Int.MaxValue / 2
  val base = new Sublist(0, null)
  base.next = new Sublist(1, base)
  base.previous = base.next

  base.next.base.end = base.base

  def after(t: Timestamp, node: Node): Timestamp = {
    val previousSublist =
      if (t == null) {
        base.next
      } else {
        t.sublist
      }

    val newTimestamp = previousSublist.after(t, node)
    if (previousSublist.size > 63) {
      val newSublist = sublistAfter(previousSublist)
      assert(previousSublist.id != newSublist.id)
      previousSublist.split(newSublist)
    }

    newTimestamp
  }

  def append(node: Node): Timestamp = {
    val newTimestamp =
      if (base.previous.size > 31) {
        val newSublist = sublistAppend()
        newSublist.append(node)
      } else {
        base.previous.append(node)
      }

    newTimestamp
  }

  private def sublistAppend(): Sublist = {
    val previous = base.previous
    val newSublist = new Sublist(previous.id + 1, base)
    newSublist.previous = previous

    previous.next = newSublist
    base.previous = newSublist

    newSublist
  }

  private def sublistAfter(s: Sublist): Sublist = {
    var node = s.next
    while (node != base) {
      node.id += 1
      node = node.next
    }
    val newSublist = new Sublist(s.id + 1, s.next)
    newSublist.previous = s

    s.next = newSublist
    newSublist.next.previous = newSublist

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

    if (t.sublist.size == 0) {
      t.sublist.previous.next = t.sublist.next
      t.sublist.next.previous = t.sublist.previous
    }
  }

  def getMods(): Iterable[ModId] = {
    val mods = Buffer[ModId]()

    var time = base.next.base.next
    while (time < base.previous.base.previous) {
      val node = time.node

      if (time.end != null) {
        node match {
          case modNode: ModNode =>
            if (modNode.modId1 != -1) {
              mods += modNode.modId1
            }
            if (modNode.modId2 != -1) {
              mods += modNode.modId2
            }
          case _ =>
        }
      }

      time = time.getNext()
    }

    mods
  }

  def splice(start: Timestamp, end: Timestamp, c: thomasdb.Context) {
    var time = start
    while (time < end) {
      val node = time.node

      if (time.end != null) {
        node match {
          case readNode: ReadNode =>
            c.ddg.reads(readNode.modId) -= time
            readNode.updated = false
          case read2Node: Read2Node =>
            c.ddg.reads(read2Node.modId1) -= time
            c.ddg.reads(read2Node.modId2) -= time
            read2Node.updated = false
          case memoNode: MemoNode =>
            memoNode.memoizer.removeEntry(time, memoNode.signature)
          case modNode: ModNode =>
            if (modNode.modizer != null) {
              if (modNode.modizer.remove(modNode.key, c)) {
                if (modNode.modId1 != -1) {
                  c.remove(modNode.modId1)
                }

                if (modNode.modId2 != -1) {
                  c.remove(modNode.modId2)
                }
              }
            } else {
              if (modNode.modId1 != -1) {
                c.remove(modNode.modId1)
              }

              if (modNode.modId2 != -1) {
                c.remove(modNode.modId2)
              }
            }

          case parNode: ParNode =>
            parNode.updated = false
          case putNode: PutNode =>
            putNode.input.remove(putNode.key, putNode.value)
          case x => println("Tried to splice unknown node type " + x)
        }

        if (time.end > end) {
          remove(time.end)
        }
      }

      time = time.getNext()
    }

    if (start.sublist == end.sublist) {
      start.previous.next = end
      end.previous = start.previous

      var size = 0
      var stamp = start.sublist.base.next
      while (stamp != start.sublist.base) {
        size += 1
        stamp = stamp.next
      }
      start.sublist.size = size
    } else {
      val startSublist =
        if (start.previous == start.sublist.base) {
          start.sublist.previous
        } else {
          start.previous.next = start.sublist.base
          start.sublist.base.previous = start.previous

          var size = 0
          var stamp = start.sublist.base.next
          while (stamp != start.sublist.base) {
            size += 1
            stamp = stamp.next
          }
          start.sublist.size = size

          start.sublist
        }

      end.previous = end.sublist.base
      end.sublist.base.next = end

      var size = 0
      var stamp = end.sublist.base.next
      while (stamp != end.sublist.base) {
        size += 1
        stamp = stamp.next
      }
      end.sublist.size = size

      startSublist.next = end.sublist
      end.sublist.previous = startSublist
    }
  }

  def getChildren(start: Timestamp, end: Timestamp): Buffer[Timestamp] = {
    println("getChildren " + start + " " + end)
    val children = Buffer[Timestamp]()

    var time = start.getNext()
    while (time != end) {
      println(time + " " + end)
      children += time
      time = time.end.getNext()
    }

    children
  }

  override def toString = {
    var node = base.next
    var ret = base.toString

    while (node != base) {
      print(node + " ")
      ret += ", " + node
      node = node.next
    }
    ret
  }
}

