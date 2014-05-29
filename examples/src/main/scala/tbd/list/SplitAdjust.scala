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

import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map

import tbd.{Adjustable, Mutator, TBD}
import tbd.mod.{AdjustableList, Mod}

object SplitAdjust {
  def predicate(pair: (Int, String)): Boolean = {
    pair._2.length % 2 == 0
  }
}

class SplitAdjust(
    partitions: Int,
    parallel: Boolean,
    memoized: Boolean) extends Algorithm(parallel, memoized) {
  var output: SplitResult = null

  var traditionalAnswer: (GenIterable[String], GenIterable[String]) = null
  type SplitResult = Mod[(AdjustableList[Int, String], AdjustableList[Int, String])]

  def run(tbd: TBD): SplitResult = {
    val pages = tbd.input.getAdjustableList[Int, String](partitions)

    pages.split(tbd, (tbd: TBD, pair:(Int, String)) => SplitAdjust.predicate(pair),
                parallel, memoized)
  }

  def traditionalRun(input: GenIterable[String]) {
     traditionalAnswer = input.partition(value => {
      SplitAdjust.predicate((0, value))
    })
  }

  def initialRun(mutator: Mutator) {
    output = mutator.run[SplitResult](this)
  }

  def checkOutput(input: GenMap[Int, String]): Boolean = {
    val sortedOutputA = output.read()._1.toBuffer().sortWith(_ < _)
    val sortedOutputB = output.read()._2.toBuffer().sortWith(_ < _)

    traditionalRun(input.values)

    sortedOutputA == traditionalAnswer._1.toBuffer.sortWith(_ < _)
    sortedOutputB == traditionalAnswer._2.toBuffer.sortWith(_ < _)
  }
}
