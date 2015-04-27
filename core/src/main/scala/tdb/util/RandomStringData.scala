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

import scala.collection.GenIterable
import scala.collection.mutable.{ArrayBuffer, Map}

import tdb.{Input, Mutator}

class RandomStringData
    (input: Input[String, String],
     count: Int,
     mutations: List[String],
     check: Boolean,
     runs: List[String]) extends Data[Int, String] {

  val maxKey = count * 100

  val rand = new scala.util.Random()

  val file = "data.txt"

  val words = Array("apple", "boy", "cat", "dog", "ear", "foo")

  var remainingRuns = runs

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
      input.put(pair._1.toString, pair._2)
    }
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

  private def getRandomKey() =
    table.keys.drop(rand.nextInt(table.size - 1)).head

  def addValue() {
    println("add")
    var key = rand.nextInt(maxKey)
    val value = makeString()
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key.toString, value)
    log("adding " + (key, value))

    if (check) {
      table += (key -> value)
    } else {
      table += (key -> "")
    }
    println("done add")
  }

  def removeValue() {
    println("remove")
    if (table.size > 1) {
      var key = getRandomKey()
      input.remove(key.toString, table(key))
      log("removing " + (key, table(key)))
      table -= key
    } else {
      addValue()
    }
    println("done remove")
  }

  def updateValue() {
    println("update")
    if (table.size > 1) {
      val key = getRandomKey()
      val value = makeString()
      input.put(key.toString, value)
      log("updating " + (key, value))
      if (check) {
        table(key) = value
      }
    } else {
      addValue()
    }
    println("done update")
  }

  def hasUpdates() = remainingRuns.size > 0
}
