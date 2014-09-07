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

    for (i <- 1 to count) {
      table(i) = generateEdges(1 to count)
      log(i + " -> " + table(i).mkString(","))
    }

    log("---")
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
      input.put(key, value)
    }
  }

  def clearValues() {
  }

  def update(n: Int) {
    var key = rand.nextInt(maxKey)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    table(key) = generateEdges()
    log(key + " -> " + table(key).mkString(","))
    input.update(key, table(key))
  }

  def hasUpdates() = true
}
