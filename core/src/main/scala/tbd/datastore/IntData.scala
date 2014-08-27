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
package tbd.datastore

import java.io._

import tbd.Input

class IntData(
    input: Input[Int, Int],
    count: Int,
    mutations: Array[String] = Array("insert", "update", "remove")
  ) extends Data[Int] {
  val maxKey = count * 10

  val rand = new scala.util.Random()

  val output = new PrintWriter(new File("data.txt"))

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
    for (pair <- table) {
      input.put(pair._1, pair._2)
    }
  }

  def clearValues() {}

  def update(n: Int) {
    for (i <- 1 to n) {
      mutations(rand.nextInt(mutations.size)) match {
	case "insert" => addValue()
	case "remove" => removeValue()
	case "update" => updateValue()
      }
    }
    log("---")
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
      input.remove(key)

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

      input.update(key, value)

      table(key) = value
    } else {
      addValue()
    }
  }

  def hasUpdates() = true

  private def log(s: String) {
    output.print(s + "\n")
    output.flush()
  }
}
