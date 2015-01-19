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
package thomasdb.util

import java.io.File
import scala.collection.mutable.Buffer

import thomasdb.Mutator
import thomasdb.list.Dataset

class FileData
    (mutator: Mutator,
     dataset: Dataset[String, String],
     inputFile: String,
     updateFile: String,
     runs: List[String]) extends Data[String] {
  val file = "data.txt"

  var remainingRuns = runs

  val updates = Buffer[(String, String)]()

  private def getMoreUpdates() {
    val file = new File(updateFile)
    val fileSize = file.length()

    val process = (key: String, value: String) => {
      updates += ((key, value))
      ()
    }

    FileUtil.readKeyValueFile(updateFile, fileSize, 0, fileSize / 100, process)
  }

  def generate() {}

  def load() {
    mutator.loadFile(inputFile, dataset)
  }

  def update() {
    val updateCount = remainingRuns.head.toInt

    remainingRuns = remainingRuns.tail

    for (i <- 1 to updateCount) {
      if (updates.size == 0) {
        getMoreUpdates()
      }

      dataset.put(updates.head._1, updates.head._2)
      updates -= updates.head
    }
  }

  def hasUpdates(): Boolean = remainingRuns.size > 0
}
