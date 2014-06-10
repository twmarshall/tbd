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


import scala.collection.GenIterable
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.{Input, Mutator}

class IntData(input: Input[Int, Int], count: Int, mutations: Array[String])
    extends Data[Int] {
  val maxKey = count * 10

  val table = Map[Int, Int]()

  val rand = new scala.util.Random()

  {
    var i = 0
    while (table.size < count) {
      val value = rand.nextInt(Int.MaxValue)
      table += (i -> value)
      i += 1
    }
  }

  def prepareNaive(parallel: Boolean): GenIterable[Int] =
    if(parallel)
      Vector[Int](table.values.toSeq: _*).par
    else
      Vector[Int](table.values.toSeq: _*)

  def loadInitial() {
    for (pair <- table) {
      input.put(pair._1, pair._2)
    }
  }

  def prepareCheck(): GenIterable[Int] =
    table.values.par

  def update() {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValue()
      case "remove" => removeValue()
      case "update" => updateValue()
    }
  }

  def addValue() {
    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key, value)

    table += (key -> value)
  }

  def removeValue() {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      input.remove(key)
      table -= key
    } else {
      addValue()
    }
  }

  def updateValue() {
    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.update(key, value)

    table(key) = value
  }
}