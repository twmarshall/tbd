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
      for ((timePtr, _value) <- c.memoTable(signature)) {
        if (!found && Timestamp.>=(timePtr, c.reexecutionStart) &&
            Timestamp.<(timePtr, c.reexecutionEnd)) {
          val value = _value.asInstanceOf[T]
          updateChangeables(timePtr, value)

          found = true

          if (Timestamp.<(c.reexecutionStart, timePtr)) {
            c.ddg.splice(c.reexecutionStart, timePtr, c)
          }

          // This ensures that we won't match anything under the currently
          // reexecuting read that comes before this memo node, since then
          // the timestamps would be out of order.
          val endPtr = Timestamp.getEndPtr(timePtr)
          c.reexecutionStart = Timestamp.getNext(endPtr)
          c.currentTime = Timestamp.getEndPtr(timePtr)

          ret = value

          val future = c.task.propagate(timePtr, endPtr)
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

      val timePtr = c.ddg.nextTimestamp(memoNodePointer, c)

      val value = func

      val endPtr = c.ddg.nextTimestamp(memoNodePointer, c)
      Timestamp.setEndPtr(timePtr, endPtr)

      if (c.memoTable.contains(signature)) {
        c.memoTable(signature) += ((timePtr, value))
      } else {
        c.memoTable += (signature -> Buffer((timePtr, value)))
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

  private def updateChangeables(timePtr: Pointer, value: T) {
    val nodePtr = Timestamp.getNodePtr(timePtr)
    value match {
      case changeable: Changeable[_] =>
        if (Node.getCurrentModId1(nodePtr) != c.currentModId) {
          c.update(c.currentModId, c.readId(changeable.modId))

          replaceMods(
            timePtr, Node.getCurrentModId1(nodePtr), c.currentModId)
        }

      case (c1: Changeable[_], c2: Changeable[_]) =>
        if (Node.getCurrentModId1(nodePtr) != c.currentModId) {
          c.update(c.currentModId, c.readId(c1.modId))

          replaceMods(
            timePtr, Node.getCurrentModId1(nodePtr), c.currentModId)
        }

        if (Node.getCurrentModId2(nodePtr) != c.currentModId2) {
          c.update(c.currentModId2, c.readId(c2.modId))

          replaceMods(
            timePtr, Node.getCurrentModId2(nodePtr), c.currentModId2)
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
      (_timePtr: Pointer,
       toReplace: ModId,
       newModId: ModId) {
    val endPtr = Timestamp.getEndPtr(_timePtr)
    var timePtr = _timePtr
    while (Timestamp.<(timePtr, endPtr)) {
      val timeEndPtr = Timestamp.getEndPtr(timePtr)
      if (timeEndPtr != -1) {
        val nodePtr = Timestamp.getNodePtr(timePtr)
        val currentModId = Node.getCurrentModId1(nodePtr)
        val currentModId2 = Node.getCurrentModId2(nodePtr)

        if (currentModId == toReplace) {

          replace(timePtr, nodePtr, toReplace, newModId)

          Node.setCurrentModId1(nodePtr, newModId)
        } else if (currentModId2 == toReplace) {
          if (timeEndPtr != -1) {
            replace(timePtr, nodePtr, toReplace, newModId)

            Node.setCurrentModId2(nodePtr, newModId)
          }
        } else {
          // Skip the subtree rooted at this node since this node doesn't have
          // toReplace as its currentModId and therefore no children can either.
          timePtr = timeEndPtr
        }
      }

      timePtr = Timestamp.getNext(timePtr)
    }
  }

  private def replace
      (timePtr: Pointer,
       nodePtr: Pointer,
       toReplace: ModId,
       newModId: ModId) {
    Node.getType(nodePtr) match {
      case Node.MemoNodeType =>
        val signature = MemoNode.getSignature(nodePtr)

        var value: T = null.asInstanceOf[T]
        for ((_timePtr, _value) <- c.memoTable(signature)) {
          if (_timePtr == timePtr) {
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
        val signature = Memo1Node.getSignature(nodePtr)

        var value: T = null.asInstanceOf[T]
        for ((_timePtr, _value) <- c.memoTable(signature)) {
          if (_timePtr == timePtr) {
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

  def removeEntry(timePtr: Pointer, signature: Seq[_]) {
    var toRemove: (Pointer, Any) = null

    for ((_timePtr, value) <- c.memoTable(signature)) {
      if (toRemove == null && _timePtr == timePtr) {
        toRemove = (_timePtr, value)
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
