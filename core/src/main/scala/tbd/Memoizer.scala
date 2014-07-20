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

class Memoizer[T](tbd: TBD, memoId: Int) {
  import tbd.worker.context.dispatcher

  def apply(args: Any*)(func: => T): T = {
    val signature = memoId +: args

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
      val memoNode = tbd.worker.ddg.addMemo(tbd.currentParent, signature)
      val outerParent = tbd.currentParent
      tbd.currentParent = memoNode
      val value = func
      tbd.currentParent = outerParent
      memoNode.endTime = tbd.worker.ddg.nextTimestamp(memoNode)
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
  override def apply(args: Any*)(func: => T): T = {
    func
  }
}
