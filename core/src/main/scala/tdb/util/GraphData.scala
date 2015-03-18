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
import scala.collection.mutable.Map

import tdb.list.ListInput

class GraphData
    (input: ListInput[Int, Array[Int]],
     count: Int,
     mutations: List[String],
     runs: List[String],
     val file: String = "data.txt") extends Data[Int, Array[Int]] {

  val averageDegree = 14

  val rand = new scala.util.Random()

  var remainingRuns = runs

  def generate() {
    for (i <- 0 until count) {
      table(i) = generateEdges(0 until count)
      for (edge <- table(i)) {
        log(i + "\t" + edge)
      }
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

  def update() = {
    val run = remainingRuns.head
    val updateCount =
      if (run.toDouble < 1)
         (run.toDouble * count).toInt
      else
        run.toInt

    remainingRuns = remainingRuns.tail

    for (i <- 0 until updateCount) {
      var key = rand.nextInt(count)

      val oldEdges = table(key)
      val newEdges =
        rand.nextInt(2) match {
          case 0 =>
            if (oldEdges.size > 0) {
              val removedEdge = oldEdges(rand.nextInt(oldEdges.size))
              log("remove\t" + key + "\t" + removedEdge)
              oldEdges.filter(_ != removedEdge)
            } else {
              val newEdge = rand.nextInt(count)
              log("add\t" + key + "\t" + newEdge)
              oldEdges :+ newEdge
            }
          case 1 =>
            var newEdge = rand.nextInt(count)
            while (oldEdges.contains(newEdge)) {
              newEdge = rand.nextInt(count)
            }
            log("add\t" + key + "\t" + newEdge)
            oldEdges :+ newEdge
        }

      table(key) = newEdges
      input.put(key, table(key))
    }

    updateCount
  }

  def hasUpdates() = remainingRuns.size > 0
}
