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

import scala.collection.mutable.ArrayBuffer

class SimpleMap(
    count: Int,
    parallel: Boolean) extends ControlAlgorithm {
  val chunks = ArrayBuffer[String]()

  def run(): Long = {
    def generate(i: Int): Vector[String] = {
      if (i == count) {
        Vector[String]()
      } else {
        if (chunks.size == 0) {
	  chunks ++= Experiment.loadPages()
        }

        val elem = chunks.head
        chunks -= elem
        generate(i + 1) :+ elem
      }
    }

    val vector =
      if (parallel) {
        generate(0).par
      } else {
        generate(0)
      }

    //Force evaulation of expression "vector" - Scala seemed to do some
    //kind of lazy evaluation during the benchmark. 
    vector

    val before = System.currentTimeMillis()
    vector.map((s: String) => MapAdjust.mapper(null, (0, s))._2)

    System.currentTimeMillis() - before
  }
}
