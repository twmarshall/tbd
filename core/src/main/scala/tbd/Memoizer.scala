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
import tbd.ddg.MemoNode
import tbd.master.Master
import tbd.macros.{TbdMacros, functionToInvoke}
import tbd.datastore.DependencyManager
import tbd.ddg.{FunctionTag, MemoNode, Tag, Timestamp}
import tbd.worker.Worker

class Memoizer[T](implicit c: Context) {
  val memoTable = Map[Seq[Any], ArrayBuffer[MemoNode]]()

  import c.worker.context.dispatcher

  def apply(signature: Any*)(func: => T): T = {
    var found = false
    var ret = null.asInstanceOf[T]
    if (!c.initialRun && !updated(signature) && memoTable.contains(signature)) {
        // Search through the memo entries matching this signature to see if
        // there's one in the right time range.
        for (memoNode <- memoTable(signature)) {
          val timestamp = memoNode.timestamp
          if (!found && timestamp > c.reexecutionStart &&
	      timestamp < c.reexecutionEnd &&
	      memoNode.matchableInEpoch <= Master.epoch) {

            updateChangeables(memoNode, c.currentMod, c.currentMod2)

            found = true
            c.ddg.attachSubtree(c.currentParent, memoNode)

	    memoNode.matchableInEpoch = Master.epoch + 1
            ret = memoNode.value.asInstanceOf[T]

	    // This ensures that we won't match anything under the currently
	    // reexecuting read that comes before this memo node, since then
	    // the timestamps would be out of order.
	    c.reexecutionStart = memoNode.endTime

            val future = c.worker.propagate(timestamp,
                                          memoNode.endTime)
            Await.result(future, DURATION)
	}
      }
    }

    if (!found) {
      val memoNode = c.ddg.addMemo(c.currentParent, signature, this)

      val outerParent = c.currentParent
      c.currentParent = memoNode
      val value = func
      c.currentParent = outerParent
      memoNode.currentMod = c.currentMod
      memoNode.currentMod2 = c.currentMod2
      memoNode.endTime = c.ddg.nextTimestamp(memoNode)
      memoNode.value = value

      if (memoTable.contains(signature)) {
        memoTable(signature) += memoNode
      } else {
        memoTable += (signature -> ArrayBuffer(memoNode))
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
      memoNode: MemoNode,
      currentMod: Mod[Any],
      currentMod2: Mod[Any]) {
    if (memoNode.currentMod != currentMod &&
	memoNode.value.isInstanceOf[Changeable[_]]) {
      val changeable = memoNode.value.asInstanceOf[Changeable[Any]]

      if (currentMod.update(changeable.mod.read())) {
	c.pending += DependencyManager.modUpdated(currentMod.id, c.worker.self)
	if (c.ddg.reads.contains(currentMod.id)) {
          c.ddg.modUpdated(currentMod.id)
          c.updatedMods += currentMod.id
	}
      }

      c.ddg.replaceMods(
	memoNode,
	memoNode.currentMod,
	currentMod)
    }

    if (memoNode.value.isInstanceOf[Tuple2[_, _]]) {
      val tuple = memoNode.value.asInstanceOf[Tuple2[Any, Any]]

      if (tuple._1.isInstanceOf[Changeable[_]] &&
	  memoNode.currentMod != currentMod) {
        val changeable = tuple._1.asInstanceOf[Changeable[Any]]

        if (currentMod.update(changeable.mod.read())) {
          c.pending += DependencyManager.modUpdated(currentMod.id, c.worker.self)
          if (c.ddg.reads.contains(currentMod.id)) {
            c.ddg.modUpdated(currentMod.id)
            c.updatedMods += currentMod.id
          }
	}

        c.ddg.replaceMods(
	  memoNode,
	  memoNode.currentMod,
	  currentMod)
      }

      if (tuple._2.isInstanceOf[Changeable[_]] &&
	  memoNode.currentMod2 != currentMod2) {
        val changeable = tuple._2.asInstanceOf[Changeable[Any]]

        if (currentMod2.update(changeable.mod.read())) {
          c.pending += DependencyManager.modUpdated(currentMod2.id, c.worker.self)
          if (c.ddg.reads.contains(currentMod2.id)) {
            c.ddg.modUpdated(currentMod2.id)
            c.updatedMods += currentMod2.id
          }
	}

        c.ddg.replaceMods(
	  memoNode,
	  memoNode.currentMod2,
	  currentMod2)
      }
    }
  }

  def removeEntry(timestamp: Timestamp, signature: Seq[_]) {
    var toRemove: MemoNode = null
    for (memoNode <- memoTable(signature)) {
      if (toRemove == null && memoNode.timestamp == timestamp) {
        toRemove = memoNode
      }
    }

    memoTable(signature) -= toRemove

    if (memoTable(signature).size == 0) {
      memoTable -= signature
    }
  }
}

class DebugMemoizer[T](implicit c: Context) extends Memoizer[T]()(c) {
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  override def apply(args: Any*)(func: => T): T = macro TbdMacros.memoMacro[T]

  def applyInternal(
      signature: Seq[_],
      func: => T,
      funcId: Int,
      freeTerms: List[(String, Any)]): T = {
    val ret = super.apply(signature)(func)

    val memoNode = c.currentParent.children.last.asInstanceOf[MemoNode]
    val tag = Tag.Memo(FunctionTag(funcId, freeTerms), signature)
    memoNode.tag = tag

    ret
  }
}

class DummyMemoizer[T](implicit c: Context) extends Memoizer[T]()(c) {
  override def apply(signature: Any*)(func: => T): T = {
    func
  }
}
