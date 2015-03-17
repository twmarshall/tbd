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

import tdb.list.ListInput

class GraphFileData
    (input: ListInput[Int, Array[Int]],
     fileName: String,
     runs: List[String],
     updateRepeat: Int) extends Data[Array[Int]] {
  val in = new BufferedReader(new FileReader(fileName))

  val file = "data2.txt"

  var remainingRuns = for (run <- runs; i <- 0 until updateRepeat) yield run

  def generate() {
    var line = in.readLine()
    while (line != "---") {
      val split = line.split("\t")
      assert(split.size == 2)

      val key = split(0).toInt
      val value = split(1).toInt

      if (!table.contains(key)) {
        table(key) = Array[Int](value)
      } else {
        table(key) :+= value
      }

      // Ensures that nodes with no outgoing edges end up in the table.
      if (!table.contains(value)) {
        table(value) = Array[Int]()
      }

      line = in.readLine()
    }
  }

  def load() = {
    for ((key, value) <- table) {
      input.put(key, value)
    }
  }

  def update() = {
    val run = remainingRuns.head
    val updateCount =
      if (run.toDouble < 1)
         (run.toDouble * table.size).toInt
      else
        run.toInt

    remainingRuns = remainingRuns.tail

    for (i <- 0 until updateCount) {
      val line = in.readLine()
      val split = line.split("\t")
      assert(split.size == 3)
      val key = split(1).toInt
      val value = split(2).toInt

      split(0) match {
        case "add" =>
          table(key) :+= value
        case "remove" =>
          table(key) = table(key).filter(_ != value)
      }

      input.put(key, table(key))
    }

    updateCount
  }

  def hasUpdates(): Boolean = remainingRuns.size > 0
}
