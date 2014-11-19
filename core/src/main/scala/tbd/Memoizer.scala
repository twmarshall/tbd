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

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.ddg.{MemoNode, Timestamp}
import tbd.master.Master

class Memoizer[T](implicit c: Context) {
  val memoTable = Map[Seq[Any], ArrayBuffer[Timestamp]]()

  import c.task.context.dispatcher

  def apply(signature: Any*)(func: => T): T = {
    var found = false
    var ret = null.asInstanceOf[T]
    if (!c.initialRun && !updated(signature) && memoTable.contains(signature)) {
      // Search through the memo entries matching this signature to see if
      // there's one in the right time range.
      for (timestamp <- memoTable(signature)) {
        val memoNode = timestamp.node.asInstanceOf[MemoNode]

        if (!found && timestamp >= c.reexecutionStart &&
      timestamp < c.reexecutionEnd) {
          updateChangeables(timestamp, c.currentMod, c.currentMod2)

          found = true

          if (c.reexecutionStart < timestamp) {
            c.ddg.ordering.splice(c.reexecutionStart, timestamp, c)
          }

          // This ensures that we won't match anything under the currently
          // reexecuting read that comes before this memo node, since then
          // the timestamps would be out of order.
          c.reexecutionStart = timestamp.end.getNext()
          c.currentTime = timestamp.end

          ret = memoNode.value.asInstanceOf[T]

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

      memoNode.currentMod = c.currentMod
      memoNode.currentMod2 = c.currentMod2
      timestamp.end = c.ddg.nextTimestamp(memoNode, c)
      memoNode.value = value

      if (memoTable.contains(signature)) {
        memoTable(signature) += timestamp
      } else {
        memoTable += (signature -> ArrayBuffer(timestamp))
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

  private def updateChangeables(
      timestamp: Timestamp,
      currentMod: Mod[Any],
      currentMod2: Mod[Any]) {
    val memoNode = timestamp.node.asInstanceOf[MemoNode]

    memoNode.value match {
      case changeable: Changeable[_] =>
        if (memoNode.currentMod != currentMod) {
          c.update(currentMod.id, c.read(changeable.mod))

          c.ddg.replaceMods(
            timestamp, memoNode, memoNode.currentMod, currentMod)
        }

      case (c1: Changeable[_], c2: Changeable[_]) =>
        if (memoNode.currentMod != currentMod) {
          c.update(currentMod.id, c.read(c1.mod))

          c.ddg.replaceMods(
            timestamp, memoNode, memoNode.currentMod, currentMod)
        }

        if (memoNode.currentMod2 != currentMod2) {
          c.update(currentMod2.id, c.read(c2.mod))

          c.ddg.replaceMods(
            timestamp, memoNode, memoNode.currentMod2, currentMod2)
        }

      case _ =>
    }
  }

  def removeEntry(timestamp: Timestamp, signature: Seq[_]) {
    var toRemove: Timestamp = null

    for (_timestamp <- memoTable(signature)) {
      if (toRemove == null && _timestamp == timestamp) {
        toRemove = _timestamp
      }
    }

    memoTable(signature) -= toRemove

    if (memoTable(signature).size == 0) {
      memoTable -= signature
    }
  }
}

class DummyMemoizer[T](implicit c: Context) extends Memoizer[T]()(c) {
  override def apply(signature: Any*)(func: => T): T = {
    func
  }
}
