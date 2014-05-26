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

import scala.collection.mutable.{Map, Buffer}

import tbd.{Adjustable, Mutator, TBD}
import tbd.mod.AdjustableList

object ChunkMapAdjust {
  def mapper(tbd: TBD, pair: (Int, String)): (Int, Int) = {
    var count = 0
    for (word <- pair._2.split("\\W+")) {
      count += 1
    }
    (pair._1, count)
  }
}

class ChunkMapAdjust(
    partitions: Int,
    chunkSize: Int,
    parallel: Boolean,
    memoized: Boolean) extends Algorithm {
  var output: AdjustableList[Int, Int] = null

  var traditionalAnswer: Buffer[Int] = null

  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val pages = tbd.input.getAdjustableList[Int, String](partitions, chunkSize, _ => 1)
    pages.map(tbd, MapAdjust.mapper, parallel = parallel, memoized = memoized)
  }

  def initialRun(mutator: Mutator) {
    output = mutator.run[AdjustableList[Int, Int]](this)
  }

  def traditionalRun(input: Map[Int, String]) {
    traditionalAnswer = input.par.map(pair => {
      MapAdjust.mapper(null, 0, pair._2)._2
    }).toBuffer
  }

  def checkOutput(input: Map[Int, String]): Boolean = {
    val sortedOutput = output.toBuffer().sortWith(_ < _)

    traditionalRun(input)

    return sortedOutput == traditionalAnswer.sortWith(_ < _)
  }
}
