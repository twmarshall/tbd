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

import scala.collection.mutable.Map

import tbd.{Adjustable, Mutator, TBD}
import tbd.mod.{AdjustableList, Mod}

class WCAdjust(
    partitions: Int,
    chunkSize: Int,
    parallel: Boolean) extends Algorithm {
  var output: Mod[(Int, scala.collection.immutable.HashMap[String, Int])] = null

  def initialRun(mutator: Mutator) {
    output = mutator.run[Mod[(Int, scala.collection.immutable.HashMap[String, Int])]](this)
  }

  def checkOutput(chunks: Map[Int, String]): Boolean = {
    val answer = chunks.par.map(value => WC.wordcount(value._2)).reduce(WC.reduce)
    output.read()._2 == answer
  }

  def mapper(tbd: TBD, pair: (Int, String)) = (pair._1, WC.wordcount(pair._2))

  def reducer(tbd: TBD, pair1: (Int, scala.collection.immutable.HashMap[String, Int]), pair2: (Int, scala.collection.immutable.HashMap[String, Int])) =
    (pair1._1, WC.reduce(pair1._2, pair2._2))

  def run(tbd: TBD): Mod[(Int, scala.collection.immutable.HashMap[String, Int])] = {
    val pages = tbd.input.getAdjustableList[Int, String](partitions, chunkSize = chunkSize, chunkSizer = _ => 1)
    val counts = pages.map(tbd, mapper, parallel = parallel)
    val initialValue = tbd.createMod((0, scala.collection.immutable.HashMap[String, Int]()))
    counts.reduce(tbd, initialValue, reducer, parallel = parallel)
  }
}
