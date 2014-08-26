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
import scala.collection.mutable.Map

import tbd.list.ListInput

class GraphData(
    input: ListInput[Int, Array[Int]],
    count: Int,
    mutations: Array[String]
  ) extends Data[Array[Int]] {

  val maxKey = count * 100

  val averageDegree = 5

  val rand = new scala.util.Random()

  def generate() {
    /*val lines = io.Source.fromFile("graph.txt").getLines
    for (line <- lines) {
      val tokens = line.split("\\s+")

      if (tokens.size != 2) {
	println("WARNING: Graph input " + line + " not formatted correctly.")
      }

      val start = tokens(0).toInt
      val end = tokens(1).toInt

      if (table.contains(start)) {
	table(start) :+= end
      } else {
	table(start) = Array(end)
      }
    }*/

    var sum = 0.0
    for (i <- 1 to count) {
      table(i) = generateEdges(1 to count)
      sum += table(i).size
    }
  }

  private def generateEdges(keys: Iterable[Int] = table.keys) = {
    var edges = Array[Int]()

    val n = keys.size / averageDegree
    for (key <- keys) {
      if (rand.nextInt(n) == 0) {
	edges :+= key
      }
    }

    edges
  }

  def load() {
    for ((key, value) <- table) {
      if (value.size > 0) {
	input.put(key, value)
      }
    }
  }

  def clearValues() {
  }

  def update() {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValue()
      case "remove" => removeValue()
      case "update" => updateValue()
    }
  }

  private def addValue() {
    var key = rand.nextInt(maxKey)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    table(key) = generateEdges()
    input.put(key, table(key))
  }

  private def removeValue() {
    if (table.size > 0) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
	key = rand.nextInt(maxKey)
      }
      table -= key
      input.remove(key)
    } else {
      addValue()
    }
  }

  private def updateValue() {
    var key = rand.nextInt(maxKey)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    table(key) = generateEdges()
    input.update(key, table(key))
  }
}
