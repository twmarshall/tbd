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

import tdb.list.ListInput

class GraphFileData
    (input: ListInput[Int, Array[Int]],
     fileName: String) extends Data[Array[Int]] {
  var lines = scala.io.Source.fromFile(fileName).getLines().toBuffer

  val file = "data2.txt"

  def generate() {
    while (lines.size > 0 && !(lines.head == "---")) {
      val split = lines.head.split(" -> ")
      assert(split.size == 2)
      table += ((split(0).toInt, split(1).split(",").map(_.toInt)))
      lines = lines.tail
    }

    lines = lines.tail
    for (entry <- table) {
      println(entry._1 + " => " + entry._2.mkString(","))
    }
  }

  def load() = {
    for ((key, value) <- table) {
      input.put(key, value)
    }
  }

  def update() = {
    val split = lines.head.split(" -> ")
    assert(split.size == 2)
    val key = split(0).toInt
    val newValue = split(1).split(",").map(_.toInt)
    table += ((key, newValue))
    lines = lines.tail
    input.put(key, newValue)
    println("updating " + key + " " + newValue)

    1
  }

  def hasUpdates(): Boolean = {
    lines.size > 0
  }
}
