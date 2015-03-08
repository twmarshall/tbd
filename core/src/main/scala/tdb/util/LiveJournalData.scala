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

class LiveJournalData(input: ListInput[Int, Array[Int]]) extends Data[Array[Int]] {

  val file = "data.txt"

  def generate() {
    var lines = scala.io.Source.fromFile("livejournal.txt").getLines().toBuffer
    for (line <- lines) {
      if (!line.startsWith("#")) {
        val split = line.split("\t")
        val from = split(0).toInt
        val to = split(0).toInt

        if (table.contains(from)) {
          table(from) :+= to
        } else {
          table(from) = Array(to)
        }
      }
    }
  }

  def load() = {
    for ((key, value) <- table) {
      input.put(key, value)
    }
  }

  def update(): Int = 0

  def hasUpdates(): Boolean = false
}
