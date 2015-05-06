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

class DirectoryData
    (input: ListInput[String, String],
     inputFile: String,
     updateDir: String,
     runs: List[String],
     check: Boolean) extends Data[String, String] {
  val file = "data.txt"

  var remainingRuns = runs

  val rand = new scala.util.Random()

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

    def process(key: String, value: String) {
      input.put(key, value)
      table(key) = value
    }

    val r = rand.nextInt(5)
    FileUtil.readEntireKeyValueFile(
      updateDir + "/" + updateCount + "-" + r, process)

    updateCount
  }

  def hasUpdates(): Boolean = remainingRuns.size > 0
}
