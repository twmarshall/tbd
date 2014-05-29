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
import tbd.{Adjustable, Changeable, Mutator, TBD}
import tbd.mod.{AdjustableList, Dest, Mod}

object Main {
  def main(args: Array[String]) {

    val visualizer = new TbdVisualizer()

    val mutator = new Mutator()

    mutator.put("A", 0)
    mutator.put("B", 2)
    val output = mutator.run[Mod[(String, Int)]](new ListSplitTest())

    mutator.put("C", 1)
    mutator.propagate()

    println(output.read())
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.update("B", 3)
    mutator.put("D", 4)
    mutator.propagate()

    println(output.read())
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.put("E", -1)
    mutator.propagate()

    println(output.read())
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.shutdown()
  }
}

class ListSortTest() extends Adjustable {
  def run(tbd: TBD): (AdjustableList[String, Int]) = {
    val list = tbd.input.getAdjustableList[String, Int](partitions = 1)
    list.sort(tbd, (tbd, a, b) => {
        a._2 < b._2
    })
  }
}

class ListSplitTest() extends Adjustable {
  def run(tbd: TBD): Mod[(AdjustableList[String, Int], AdjustableList[String, Int])] = {
    val list = tbd.input.getAdjustableList[String, Int](partitions = 1)
    list.split(tbd, (tbd, a) => {
        a._2 % 2 == 0
    }, true, true)
  }
}
class ListReduceSumTest extends Adjustable {
  def run(tbd: TBD): Mod[(String, Int)] = {
    val modList = tbd.input.getAdjustableList[String, Int](partitions = 1)
    val zero = tbd.mod((dest : Dest[(String, Int)]) => tbd.write(dest, ("", 0)))
    modList.reduce(tbd, zero,
      (tbd: TBD, pair1: (String, Int), pair2: (String, Int)) => {
        (pair2._1, pair1._2 + pair2._2)
      })
  }
}
