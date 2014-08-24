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
    input: ListInput[Int, Array[Int]]
  ) extends Data[Array[Int]] {

  def generate() {
    val lines = io.Source.fromFile("graph.txt").getLines
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
    }

    for ((key, value) <- table) {
      input.put(key, value)
      println(key + " -> " + value.mkString(","))
    }
  }

  def load() {
  }

  def clearValues() {
  }

  def update() {
  }
}
