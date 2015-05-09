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

import java.io._
import scala.collection.mutable.Buffer

import tdb.Constants._
import tdb.list.ListInput

class FileData
    (input: ListInput[String, String],
     inputFile: String,
     updateFile: String,
     runs: List[String],
     check: Boolean) extends Data[String, String] {
  val file = "data.txt"

  var remainingRuns = runs

  private var updates = new BufferedReader(new FileReader(updateFile))

  def generate() {
    val process = (key: String, value: String) => {
      table(key) = value
      ()
    }

    if (check) {
      FileUtil.readEntireKeyValueFile(inputFile, process)
    }
  }

  def load() {
    input.loadFile(inputFile)
  }

  def update(): Int = {
    val updateCount = remainingRuns.head.toInt
    var gotMore = false
    remainingRuns = remainingRuns.tail

    for (i <- 1 to updateCount) {
      var line = updates.readLine()

      if (line == null) {
        println("Warning: the update file is too small, update size = " +
                updateCount)
        updates = new BufferedReader(new FileReader(updateFile))
        line = updates.readLine()
      }
      val split = line.split(unitSeparator)

      val key = split(0)
      val value = split(1)

      input.put(key, value)
      table(key) = value
    }

    updateCount
  }

  def hasUpdates(): Boolean = remainingRuns.size > 0
}
