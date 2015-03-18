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
package tdb.util

import java.io.File
import scala.collection.mutable.Buffer

import tdb.list.ListInput

class FileData
    (input: ListInput[String, String],
     inputFile: String,
     updateFile: String,
     runs: List[String]) extends Data[String, String] {
  val file = "data.txt"

  var remainingRuns = runs

  val updates = Buffer[(String, String)]()

  private def getMoreUpdates() {
    val process = (key: String, value: String) => {
      updates += ((key, value))
      ()
    }

    FileUtil.readEntireKeyValueFile(updateFile, process)
  }

  def generate() {
    val process = (key: String, value: String) => {
      table(key) = value
      ()
    }

    FileUtil.readEntireKeyValueFile(inputFile, process)
  }

  def load() {
    input.loadFile(inputFile)
  }

  def update(): Int = {
    val updateCount = remainingRuns.head.toInt

    remainingRuns = remainingRuns.tail

    for (i <- 1 to updateCount) {
      if (updates.size == 0) {
        getMoreUpdates()
      }

      val key = updates.head._1
      val value = updates.head._2
      input.put(key, value)
      table(key) = value
      updates -= updates.head
    }

    updateCount
  }

  def hasUpdates(): Boolean = remainingRuns.size > 0
}
