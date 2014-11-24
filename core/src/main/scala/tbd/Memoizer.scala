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
import tbd.ddg._
import tbd.master.Master

class Memoizer[T](implicit c: Context) {
  import c.task.context.dispatcher

  val memoizerId = c.addMemoizer(this)

  def apply(args: Any*)(func: => T): T = {
    val signature = memoizerId +: args

    var found = false
    var ret = null.asInstanceOf[T]
    if (!c.initialRun && !updated(signature) &&
        c.memoTable.contains(signature)) {
      // Search through the memo entries matching this signature to see if
      // there's one in the right time range.
      for ((timestamp, _value) <- c.memoTable(signature)) {
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
      val memoNodePointer =
        if (args.size == 1 && args(0).isInstanceOf[Mod[_]]) {
          val modId = args(0).asInstanceOf[Mod[_]].id

          Memo1Node.create(memoizerId, modId, c.currentModId, c.currentModId2)
        } else {
          MemoNode.create(
            memoizerId, signature, c.currentModId, c.currentModId2)
        }

      val timestamp = c.ddg.nextTimestamp(memoNodePointer, c)

      val value = func

      timestamp.end = c.ddg.nextTimestamp(timestamp.nodePtr, c)

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
    value match {
      case changeable: Changeable[_] =>
        if (Node.getCurrentModId1(timestamp.nodePtr) != c.currentModId) {
          c.update(c.currentModId, c.readId(changeable.modId))

          replaceMods(
            timestamp, Node.getCurrentModId1(timestamp.nodePtr), c.currentModId)
        }

      case (c1: Changeable[_], c2: Changeable[_]) =>
        if (Node.getCurrentModId1(timestamp.nodePtr) != c.currentModId) {
          c.update(c.currentModId, c.readId(c1.modId))

          replaceMods(
            timestamp, Node.getCurrentModId1(timestamp.nodePtr), c.currentModId)
        }

        if (Node.getCurrentModId2(timestamp.nodePtr) != c.currentModId2) {
          c.update(c.currentModId2, c.readId(c2.modId))

          replaceMods(
            timestamp, Node.getCurrentModId2(timestamp.nodePtr), c.currentModId2)
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
       toReplace: ModId,
       newModId: ModId) {
    var time = _timestamp
    while (time < _timestamp.end) {
      if (time.end != null) {
        val currentModId = Node.getCurrentModId1(time.nodePtr)
        val currentModId2 = Node.getCurrentModId2(time.nodePtr)

        if (currentModId == toReplace) {

          replace(time, toReplace, newModId)

          Node.setCurrentModId1(time.nodePtr, newModId)
        } else if (currentModId2 == toReplace) {
          if (time.end != null) {
            replace(time, toReplace, newModId)

            Node.setCurrentModId2(time.nodePtr, newModId)
          }
        } else {
          // Skip the subtree rooted at this node since this node doesn't have
          // toReplace as its currentModId and therefore no children can either.
          time = time.end
        }
      }

      time = time.getNext()
    }
  }

  private def replace(time: Timestamp, toReplace: ModId, newModId: ModId) {
    Node.getType(time.nodePtr) match {
      case Node.MemoNodeType =>
        val signature = MemoNode.getSignature(time.nodePtr)

        var value: T = null.asInstanceOf[T]
        for ((_time, _value) <-
             c.memoTable(signature)) {
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

      case Node.Memo1NodeType =>
        val signature = Memo1Node.getSignature(time.nodePtr)

        var value: T = null.asInstanceOf[T]
        for ((_time, _value) <- c.memoTable(signature)) {
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
