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

class IntData
    (input: ListInput[Int, Int],
     runs: List[String],
     count: Int,
     mutations: List[String] = List("insert", "update", "remove"),
     _file: String = "data.txt") extends Data[Int] {
  val maxKey = count * 10

  val rand = new scala.util.Random()

  val file = _file

  var remainingRuns = runs

  def generate() {
    while (table.size < count) {
      var key = rand.nextInt(maxKey)
      val value = rand.nextInt(maxKey)
      while (table.contains(key)) {
        key = rand.nextInt(maxKey)
      }

      table += (key -> value)
      log("generated " + (key, value))
    }
  }

  def load() {
    input.load(table)
  }

  def update() = {
    val run = remainingRuns.head
    val updateCount =
      if (run.toDouble < 1)
         (run.toDouble * count).toInt
      else
        run.toInt

    remainingRuns = remainingRuns.tail

    for (i <- 1 to updateCount) {
      mutations(rand.nextInt(mutations.size)) match {
        case "insert" => addValue()
        case "remove" => removeValue()
        case "update" => updateValue()
      }
    }
    log("---")

    updateCount
  }

  def addValue() {
    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(maxKey)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }

    input.put(key, value)

    log("adding " + (key, value))

    table += (key -> value)
  }

  def removeValue() {
    if (table.size > 0) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      input.remove(key, table(key))

      log("removing " + (key, table(key)))

      table -= key
    } else {
      addValue()
    }
  }

  def updateValue() {
    if (table.size > 0) {
      var key = rand.nextInt(maxKey)
      val value = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }

      log("updating " + (key, value))

      input.put(key, value)

      table(key) = value
    } else {
      addValue()
    }
  }

  def hasUpdates() = remainingRuns.size > 0
}
