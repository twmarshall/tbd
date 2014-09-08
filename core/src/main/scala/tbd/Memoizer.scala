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
import tbd.ddg.{FunctionTag, Timestamp}
import tbd.worker.Worker

class Memoizer[T](c: Context) {
  val memoTable = Map[Seq[Any], ArrayBuffer[MemoNode]]()

  import c.worker.context.dispatcher
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  def apply(args: Any*)(func: => T): T = macro TbdMacros.memoMacro[T]

  def applyInternal(
      signature: Seq[_],
      func: => T,
      funcId: Int,
      freeTerms: List[(String, Any)]): T = {

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

            updateChangeables(memoNode, c.worker, c.currentDest, c.currentDest2)

            found = true
            c.worker.ddg.attachSubtree(c.currentParent, memoNode)

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
      val memoNode = c.worker.ddg.addMemo(c.currentParent, signature, this,
                                          FunctionTag(funcId, freeTerms))
      val outerParent = c.currentParent
      c.currentParent = memoNode
      val value = func
      c.currentParent = outerParent
      memoNode.currentDest = c.currentDest
      memoNode.currentDest2 = c.currentDest2
      memoNode.endTime = c.worker.ddg.nextTimestamp(memoNode)
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
      worker: Worker,
      currentDest: Dest[Any],
      currentDest2: Dest[Any]) {
    if (memoNode.currentDest != currentDest &&
	memoNode.value.isInstanceOf[Changeable[_]]) {
      val changeable = memoNode.value.asInstanceOf[Changeable[Any]]

      val awaiting = currentDest.mod.update(changeable.mod.read())
      Await.result(Future.sequence(awaiting), DURATION)

      worker.ddg.replaceDests(memoNode,
			      memoNode.currentDest,
			      currentDest)
    }

    if (memoNode.value.isInstanceOf[Tuple2[_, _]]) {
      val tuple = memoNode.value.asInstanceOf[Tuple2[Any, Any]]

      if (tuple._1.isInstanceOf[Changeable[_]] &&
	  memoNode.currentDest != currentDest) {
        val changeable = tuple._1.asInstanceOf[Changeable[Any]]

        val awaiting = currentDest.mod.update(changeable.mod.read())
        Await.result(Future.sequence(awaiting), DURATION)

        worker.ddg.replaceDests(memoNode,
			        memoNode.currentDest,
			        currentDest)
      }

      if (tuple._2.isInstanceOf[Changeable[_]] &&
	  memoNode.currentDest2 != currentDest2) {
        val changeable = tuple._2.asInstanceOf[Changeable[Any]]

        val awaiting = currentDest2.mod.update(changeable.mod.read())
        Await.result(Future.sequence(awaiting), DURATION)

        worker.ddg.replaceDests(memoNode,
			        memoNode.currentDest2,
			        currentDest2)
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
  }
}

class DummyMemoizer[T](c: Context) extends Memoizer[T](c) {
  override def applyInternal(
      args: Seq[_],
      func: => T,
      fundId: Int,
      freeTerms: List[(String, Any)]): T = {
    func
  }
}
