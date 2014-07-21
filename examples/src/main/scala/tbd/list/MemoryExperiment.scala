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

import tbd.{Adjustable, ListConf, ListInput, Mutator, TBD}
import tbd.mod.AdjustableList

class MemoryExperiment(input: ListInput[Int, String]) extends Adjustable {
  val partitions = 4
  def run(implicit tbd: TBD): AdjustableList[Int, String] = {
    val list = input.getAdjustableList()
    list.map((tbd: TBD, pair: (Int, String)) => pair)
  }
}

object MemoryExperiment {
  def main(args: Array[String]) {
    val max = 1000
    val mutator = new Mutator()
    val list = mutator.createList[Int, String](new ListConf(partitions = 4))
    val input = new WCData(list, max, Array("insert", "remove", "update"))

    for (i <- 0 to max) {
      input.addValue()
    }

    val output = mutator.run[AdjustableList[Int, Int]](new MemoryExperiment(list))

    val rand = new scala.util.Random()
    var i = 0
    while (i < 1000) {
      for (i <- 0 to 100) {
        input.update()
      }

      println("starting propagating")
      mutator.propagate()
      println("done propagating")
      i += 1
    }
  }
}
