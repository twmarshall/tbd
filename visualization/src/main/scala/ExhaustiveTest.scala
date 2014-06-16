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

package tbd.visualization

import scala.collection.mutable.ArrayBuffer
import tbd.{Adjustable, Changeable, ListConf, ListInput, Mutator, TBD}
import tbd.mod.{AdjustableList, Dest, Mod}
import collection.mutable.HashMap
import scala.util.Random

class ExhaustiveTest extends TestBase {

  val initialSize = 10
  val maximalMutationsPerPropagation = 2

  def run() {

    val visualizer = new TbdVisualizer()
    var mutationCounter = 1

    for(i <- 0 to initialSize)
      addValue()

    val output = mutator.run[AdjustableList[Int, Int]](new ListQuicksortTest(input))
    mutator.propagate()

    while(true) {

      for(i <- 0 to rand.nextInt(maximalMutationsPerPropagation)) {
        randomMutation()
      }
      mutator.propagate()

      println("m.propagate() // Mutation Cycle " + mutationCounter)
      println("// Current Buffer: " + table.values.toBuffer)

      mutationCounter += 1

      val ca = table.values.toBuffer.sortWith(_ < _)
      val a = output.toBuffer()

      println("// Output: " + output)

      if(a != ca ) {
        println("Check error.")
        println("a: " + a)
        println("ca: " + ca)

        visualizer.showDDG(mutator.getDDG().root)
        while(true) { readLine() }
      }

      visualizer.showDDG(mutator.getDDG().root)
      readLine()
    }

    mutator.shutdown()
  }
}