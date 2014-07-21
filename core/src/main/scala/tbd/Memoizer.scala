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

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.ddg.MemoNode
import tbd.master.Master
import tbd.mod.Mod
import tbd.macros.TbdMacros
import tbd.ddg.FunctionTag

class Memoizer[T](tbd: TBD, memoId: Int) {
  import tbd.worker.context.dispatcher

  import scala.language.experimental.macros
  def apply(args: Any*)(func: => T): T = macro TbdMacros.memoMacro[T]

  def memoInternal(
      args: Seq[_],
      func: => T,
      funcId: Int,
      freeTerms: List[(String, Any)]): T = {
    val signature = memoId :: args.toList

    var found = false
    var ret = null.asInstanceOf[T]
    if (!tbd.initialRun) {
      if (!tbd.updated(args)) {
	if (tbd.worker.memoTable.contains(signature)) {

          // Search through the memo entries matching this signature to see if
          // there's one in the right time range.
          for (memoNode <- tbd.worker.memoTable(signature)) {
            val timestamp = memoNode.timestamp
            if (!found && timestamp > tbd.reexecutionStart &&
		timestamp < tbd.reexecutionEnd &&
		memoNode.matchableInEpoch <= Master.epoch) {

	      if (memoNode.currentDest != tbd.currentDest &&
		  memoNode.value.isInstanceOf[Changeable[_]]) {
                val changeable = memoNode.value.asInstanceOf[Changeable[Any]]

                val awaiting = tbd.currentDest.mod.update(changeable.mod.read())
                Await.result(Future.sequence(awaiting), DURATION)

		tbd.worker.ddg.replaceDests(memoNode,
					    memoNode.currentDest,
					    tbd.currentDest)
              }

	      if (memoNode.value.isInstanceOf[Changeable2[_, _]] &&
		  memoNode.currentDest2 != tbd.currentDest2) {
		val changeable2 = memoNode.value.asInstanceOf[Changeable2[Any, Any]]

		val awaiting = tbd.currentDest2.mod.update(changeable2.mod2.read())
		Await.result(Future.sequence(awaiting), DURATION)

		tbd.worker.ddg.replaceDests(memoNode,
					    memoNode.currentDest2,
					    tbd.currentDest2)
	      }

              found = true
              tbd.worker.ddg.attachSubtree(tbd.currentParent, memoNode)

	      memoNode.matchableInEpoch = Master.epoch + 1
              ret = memoNode.value.asInstanceOf[T]

	      // This ensures that we won't match anything under the currently
	      // reexecuting read that comes before this memo node, since then
	      // the timestamps would be out of order.
	      tbd.reexecutionStart = memoNode.endTime

              val future = tbd.worker.propagate(timestamp,
                                                memoNode.endTime)
              Await.result(future, DURATION)
            }
          }
	}
      }
    }

    if (!found) {
      val memoNode = tbd.worker.ddg.addMemo(tbd.currentParent, signature,
                                            FunctionTag(funcId, freeTerms))
      val outerParent = tbd.currentParent
      tbd.currentParent = memoNode
      val value = func
      tbd.currentParent = outerParent
      memoNode.endTime = tbd.worker.ddg.nextTimestamp(memoNode)
      memoNode.currentDest = tbd.currentDest
      memoNode.currentDest2 = tbd.currentDest2
      memoNode.value = value

      if (tbd.worker.memoTable.contains(signature)) {
        tbd.worker.memoTable(signature) += memoNode
      } else {
        tbd.worker.memoTable += (signature -> ArrayBuffer(memoNode))
      }

      ret = value
    }

    ret
  }
}

class DummyMemoizer[T](tbd: TBD, memoId: Int) extends Memoizer[T](tbd, memoId) {
  override def memoInternal(
      args: Seq[_],
      func: => T,
      fundId: Int,
      freeTerms: List[(String, Any)]): T = {
    func
  }
}
