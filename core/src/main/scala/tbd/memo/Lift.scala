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
package tbd.memo

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Await

import tbd.Constants._
import tbd.TBD
import tbd.mod.Mod

class Lift[T](tbd: TBD, memoId: Int) {

  def memo(aArgs: List[Mod[_]], func: () => T): T = {
    val args = aArgs.map(_.id)
    val signature = memoId :: args

    var found = false
    var toRemove: MemoEntry = null
    var ret = null.asInstanceOf[T]
    //println("looking for " + signature)
    if (!tbd.initialRun) {
      if (!tbd.updated(args)) {
	if (tbd.worker.memoTable.contains(signature)) {

          // Search through the memo entries matching this signature to see if
          // there's one in the right time range.
          for (memoEntry <- tbd.worker.memoTable(signature)) {
            val timestamp = memoEntry.node.timestamp
            if (!found && timestamp > tbd.reexecutionStart &&
		timestamp < tbd.reexecutionEnd && memoEntry.node.matchable) {
	      //println("found a match on " + signature)
              found = true
              tbd.worker.ddg.attachSubtree(tbd.currentParent, memoEntry.node)
              toRemove = memoEntry
              ret = memoEntry.value.asInstanceOf[T]

              val future = tbd.worker.propagate(timestamp,
                                                memoEntry.node.endTime)
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
      val value = func()
      tbd.currentParent = outerParent
      memoNode.endTime = tbd.worker.ddg.nextTimestamp(memoNode)

      val memoEntry = new MemoEntry(value, memoNode)

      if (tbd.worker.memoTable.contains(signature)) {
        tbd.worker.memoTable(signature) += memoEntry
      } else {
        tbd.worker.memoTable += (signature -> ArrayBuffer(memoEntry))
      }

      ret = value
    } else {
      tbd.worker.memoTable(signature) -= toRemove
    }

    ret
  }
}
