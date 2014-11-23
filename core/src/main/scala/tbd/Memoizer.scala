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
package tbd

import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.ddg.{MemoNode, Node, Timestamp}
import tbd.master.Master

class Memoizer[T](implicit c: Context) {
  import c.task.context.dispatcher

  val memoizerId = c.nextMemoizerId
  c.nextMemoizerId += 1

  def apply(args: Any*)(func: => T): T = {
    val signature = memoizerId +: args

    var found = false
    var ret = null.asInstanceOf[T]
    if (!c.initialRun && !updated(signature) &&
        c.memoTable.contains(signature)) {
      // Search through the memo entries matching this signature to see if
      // there's one in the right time range.
      for ((timestamp, _value) <- c.memoTable(signature)) {
        val memoNode = timestamp.node.asInstanceOf[MemoNode]

        if (!found && timestamp >= c.reexecutionStart &&
            timestamp < c.reexecutionEnd) {
          val value = _value.asInstanceOf[T]
          updateChangeables(timestamp, value)

          found = true

          if (c.reexecutionStart < timestamp) {
            c.ddg.splice(c.reexecutionStart, timestamp, c)
          }

          // This ensures that we won't match anything under the currently
          // reexecuting read that comes before this memo node, since then
          // the timestamps would be out of order.
          c.reexecutionStart = timestamp.end.getNext()
          c.currentTime = timestamp.end

          ret = value

          val future = c.task.propagate(timestamp, timestamp.end)
          Await.result(future, DURATION)
          future onComplete {
            case scala.util.Failure(e) => e.printStackTrace()
            case _ =>
          }
        }
      }
    }

    if (!found) {
      val timestamp = c.ddg.addMemo(signature, this, c)
      val memoNode = timestamp.node.asInstanceOf[MemoNode]

      val value = func

      memoNode.currentModId = c.currentModId
      memoNode.currentModId2 = c.currentModId2
      timestamp.end = c.ddg.nextTimestamp(memoNode, c)

      if (c.memoTable.contains(signature)) {
        c.memoTable(signature) += ((timestamp, value))
      } else {
        c.memoTable += (signature -> Buffer((timestamp, value)))
      }

      ret = value
    }

    ret
  }

  private def updated(args: Seq[_]): Boolean = {
    var updated = false

    for (arg <- args) {
      if (arg.isInstanceOf[Mod[_]]) {
        if (c.updatedMods.contains(arg.asInstanceOf[Mod[_]].id)) {
          updated = true
        }
      }
    }

    updated
  }

  private def updateChangeables(timestamp: Timestamp, value: T) {
    val memoNode = timestamp.node.asInstanceOf[MemoNode]

    value match {
      case changeable: Changeable[_] =>
        if (memoNode.currentModId != c.currentModId) {
          c.update(c.currentModId, c.readId(changeable.modId))

          replaceMods(
            timestamp, memoNode, memoNode.currentModId, c.currentModId)
        }

      case (c1: Changeable[_], c2: Changeable[_]) =>
        if (memoNode.currentModId != c.currentModId) {
          c.update(c.currentModId, c.readId(c1.modId))

          replaceMods(
            timestamp, memoNode, memoNode.currentModId, c.currentModId)
        }

        if (memoNode.currentModId2 != c.currentModId2) {
          c.update(c.currentModId2, c.readId(c2.modId))

          replaceMods(
            timestamp, memoNode, memoNode.currentModId2, c.currentModId2)
        }

      case _ =>
    }
  }

  /**
   * Replaces all of the mods equal to toReplace with newModId in the subtree
   * rooted at node. This is called when a memo match is made where the memo
   * node has a currentMod that's different from the Context's currentMod.
   */
  private def replaceMods
      (_timestamp: Timestamp,
       _node: Node,
       toReplace: ModId,
       newModId: ModId): Buffer[Node] = {
    val buf = Buffer[Node]()

    var time = _timestamp
    while (time < _timestamp.end) {
      val node = time.node

      if (time.end != null) {
        val (currentModId, currentModId2) =
          if (time.pointer != -1) {
            (Node.getCurrentModId1(time.pointer),
             Node.getCurrentModId2(time.pointer))
          } else {
            (node.currentModId, node.currentModId2)
          }

        if (currentModId == toReplace) {

          buf += time.node
          replace(time, toReplace, newModId)

          if (time.pointer != -1) {
            Node.setCurrentModId1(time.pointer, newModId)
          } else {
            node.currentModId = newModId
          }
        } else if (currentModId2 == toReplace) {
          if (time.end != null) {
            buf += time.node
            replace(time, toReplace, newModId)

            if (time.pointer != -1) {
              Node.setCurrentModId2(time.pointer, newModId)
            } else {
              node.currentModId2 = newModId
            }
          }
        } else {
          // Skip the subtree rooted at this node since this node doesn't have
          // toReplace as its currentModId and therefore no children can either.
          time = time.end
        }
      }

      time = time.getNext()
    }

    buf
  }

  private def replace(time: Timestamp, toReplace: ModId, newModId: ModId) {
    time.node match {
      case memoNode: MemoNode =>
        if (!c.memoTable.contains(memoNode.signature)) {
          println(this + " " + memoNode.signature)
          for ((signature, asdf) <- c.memoTable) {
            println("   " + signature)
          }
        }

        var value: T = null.asInstanceOf[T]
        for ((_time, _value) <- c.memoTable(memoNode.signature)) {
          if (_time == time) {
            value = _value.asInstanceOf[T]
          }
        }

        value match {
          case changeable: Changeable[_] =>
            if (changeable.modId == toReplace) {
              changeable.modId = newModId
            }
          case (c1: Changeable[_], c2: Changeable[_]) =>
            if (c1.modId == toReplace) {
              c1.modId = newModId
            }

            if (c2.modId == toReplace) {
              c2.modId = newModId
            }
          case _ =>
        }
      case _ =>
    }
  }

  def removeEntry(timestamp: Timestamp, signature: Seq[_]) {
    var toRemove: (Timestamp, Any) = null

    for ((_timestamp, value) <- c.memoTable(signature)) {
      if (toRemove == null && _timestamp == timestamp) {
        toRemove = (_timestamp, value)
      }
    }

    c.memoTable(signature) -= toRemove

    if (c.memoTable(signature).size == 0) {
      c.memoTable -= signature
    }
  }
}

class DummyMemoizer[T](implicit c: Context) extends Memoizer[T]()(c) {
  override def apply(signature: Any*)(func: => T): T = {
    func
  }
}
