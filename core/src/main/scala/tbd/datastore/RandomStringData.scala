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

import scala.collection.GenIterable
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.{Input, Mutator}

class RandomStringData(
    input: Input[Int, String],
    count: Int,
    mutations: Array[String],
    check: Boolean
  ) extends Data[String] {

  val maxKey = count * 10

  val rand = new scala.util.Random()

  val file = "data.txt"

  val words = Array("apple", "boy", "cat", "dog", "ear", "foo")

  def generate() {
    var key = 0
    while (table.size < count) {
      table(key) = makeString()
      log("generated " + (key, table(key)))
      key += 1
    }
  }

  private def makeString(): String = {
    val buf = new StringBuffer()
    for (i <- 1 until rand.nextInt(10) + 2) {
      buf.append(words(rand.nextInt(6)) + " ")
    }
    buf.toString()
  }

  def load() {
    for (pair <- table) {
      input.put(pair._1, pair._2)
    }
  }

  def clearValues() {
    for ((key, value) <- table) {
      table(key) = ""
    }
  }

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
    val value = makeString()
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key, value)
    log("adding " + (key, value))

    if (check) {
      table += (key -> value)
    } else {
      table += (key -> "")
    }
  }

  def removeValue() {
    if (table.size > 1) {
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
    var key = rand.nextInt(maxKey)
    val value = makeString()
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.update(key, value)
    log("updating " + (key, value))

    if (check) {
      table(key) = value
    }
  }

  def hasUpdates() = true
}
