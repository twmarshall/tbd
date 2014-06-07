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
    visualizer.showLabels = false

    val mutator = new Mutator()

    for(i <- 1 to 10)
      mutator.put(i.toString(), i)

    val output = mutator.run[AdjustableList[String, Int]](new ListQuicksortTest())
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.update("0", 99)
    mutator.update("1", 2)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.update("7", 4)
    mutator.update("8", -1)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.remove("1")
    mutator.remove("3")
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.remove("2")
    mutator.remove("7")
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.put("1", 33)
    mutator.put("7", 22)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.put("11", 55)
    mutator.put("12", 66)
    mutator.propagate()

    println(output)
    visualizer.showDDG(mutator.getDDG().root)
    readLine()

    mutator.shutdown()
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
class ListQuicksortTest extends Adjustable {
  def run(tbd: TBD): AdjustableList[String, Int] = {
    val modList = tbd.input.getAdjustableList[String, Int](partitions = 1)
    modList.sort(tbd, (tbd, a, b) => a._2 < b._2, true, true)
  }
}
