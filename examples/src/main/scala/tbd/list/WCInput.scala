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
package tbd.examples.list

import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.{Input, Mutator}

class WCInput(maxKey: Int, mutations: Array[String]) {
  val chunks = ArrayBuffer[String]()

  val rand = new scala.util.Random()
  def addValue(input: Input[Int, String], table: Map[Int, String]) {
    if (chunks.size == 0) {
      chunks ++= Experiment.loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key, chunks.head)

    if (Experiment.check) {
      table += (key -> chunks.head)
    } else {
      table += (key -> "")
    }

    chunks -= chunks.head
  }

  def removeValue(input: Input[Int, String], table: Map[Int, String]) {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      input.remove(key)
      table -= key
    } else {
      addValue(input, table)
    }
  }

  def updateValue(input: Input[Int, String], table: Map[Int, String]) {
    if (chunks.size == 0) {
      chunks ++= Experiment.loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.update(key, chunks.head)

    if (Experiment.check) {
      table(key) = chunks.head
    }

    chunks -= chunks.head
  }

  def update(input: Input[Int, String], table: Map[Int, String]) {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValue(input, table)
      case "remove" => removeValue(input, table)
      case "update" => updateValue(input, table)
    }
  }
}
