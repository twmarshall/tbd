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
import tbd._
import tbd.mod.{AdjustableList, Dest, Mod}

class QuicksortTest {
  def run() {

    val visualizer = new TbdVisualizer()
    visualizer.showLabels = false

    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](ListConf(partitions = 1))

    for(i <- 1 to 10)
      input.put(i, i)

    val output = mutator.run[AdjustableList[Int, Int]](new ListQuicksortTest(input))
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    input.update(0, 99)
    input.update(1, 2)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    input.update(7, 4)
    input.update(8, -1)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    input.remove(1)
    input.remove(3)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    input.remove(2)
    input.remove(7)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    input.put(1, 33)
    input.put(7, 22)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    input.put(11, 55)
    input.put(12, 66)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.shutdown()
  }
}

class ListReduceSumTest(input: ListInput[Int, Int])  extends Adjustable {
  def run(tbd: TBD): Mod[(Int, Int)] = {
    val modList = input.getAdjustableList()
    val zero = tbd.mod((dest : Dest[(Int, Int)]) => tbd.write(dest, (0, 0)))
    modList.reduce(tbd, zero,
      (tbd: TBD, pair1: (Int, Int), pair2: (Int, Int)) => {
        (pair2._1, pair1._2 + pair2._2)
      })
  }
}
class ListQuicksortTest(input: ListInput[Int, Int])  extends Adjustable {
  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val modList = input.getAdjustableList()
    modList.sort(tbd, (tbd, a, b) => a._2 < b._2, true, true)
  }
}

class ListSplitTest(input: ListInput[Int, Int])  extends Adjustable {
  def run(tbd: TBD): (AdjustableList[Int, Int], AdjustableList[Int, Int]) = {
    val modList = input.getAdjustableList()
    modList.split(tbd, (tbd, a) => a._2 % 2 == 0, true, true)
  }
}