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
import scala.util.matching.Regex

class ManualTest extends TestBase {

  val initialSize = 10
  var propagate = true

  val putm = "(a) (\\d+) (\\d+)".r
  val updatem = "(u) (\\d+) (\\d+)".r
  val remm = "(r) (\\d+)".r
  val propagatem = "(p)".r

  def run() {

    val visualizer = new TbdVisualizer()
    var mutationCounter = 1

    for(i <- 0 to initialSize)
      addValue()

    val output = mutator.run[(AdjustableList[Int, Int], AdjustableList[Int, Int])](new ListSplitTest(input))

    println("// Commands: ")
    println("// a KEY VALUE adds a key and value ")
    println("// u KEY VALUE updates a value ")
    println("// r KEY removes a key and value ")
    println("// p propagates ")
    println("")
    println("// KEY and VALUE have to be integers ")


    while(true) {

      while(!propagate) {
        readLine() match {
          case putm(_, key, value) => addValue(key.toInt, value.toInt)
          case updatem(_, key, value) => updateValue(key.toInt, value.toInt)
          case remm(_, key) => removeValue(key.toInt)
          case propagatem(_) => propagate = true
          case _ =>
        }
      }
      propagate = false
      mutator.propagate()

      println("m.propagate() // Mutation Cycle " + mutationCounter)
      println("// Current Buffer: " + table.values.toBuffer)

      mutationCounter += 1
//
//      val ca = table.values.toBuffer.sortWith(_ < _)
//
//      val a = output.toBuffer()

      println("// Output: " + output)
//
//      if(a != ca) {
//        println("Check error.")
//        println("a: " + a)
//        println("ca: " + ca)
//
//        visualizer.showDDG(mutator.getDDG().root)
//        readLine()
//      }

      visualizer.showDDG(mutator.getDDG().root)
    }

    mutator.shutdown()
  }
}