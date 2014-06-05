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
import collection.mutable.HashMap
import scala.util.Random

class ExhaustiveTest {
  val mutator = new Mutator()

  val table = new HashMap[Int, Int]
  val rand = new Random()
  var freeList = List[Int]()

  var keyCounter = 0
  val maxValue = 100

  def addValue() {
    val newValue = rand.nextInt(maxValue)
    val newKey =
      if(freeList.size == 0 || rand.nextInt(2) == 1) {
        keyCounter += 1
        keyCounter
      } else {
        var (head::tail) = freeList
        freeList = tail
        head
      }

    println("m.put(" + newKey + ", " + newValue + ")")

    mutator.put(newKey, newValue)
    table += (newKey -> newValue)
  }

  def removeValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toRemove = keys(rand.nextInt(keys.length))

      println("m.remove(" + toRemove + ") // Was " + table(toRemove))

      table -= toRemove
      mutator.remove(toRemove)
      freeList = (freeList :+ toRemove)
    }
  }

  def updateValue() {
    val keys = table.keys.toBuffer

    if(keys.length > 0)
    {
      val toUpdate = keys(rand.nextInt(keys.length))
      val newValue = rand.nextInt(maxValue)

      println("m.update(" + toUpdate + ", " + newValue + ")" +
              "// was (" + toUpdate + ", " + table(toUpdate) + ") ")

      table(toUpdate) = newValue
      mutator.update(toUpdate, newValue)
    }
  }

  def randomMutation() {
    rand.nextInt(3) match {
      case 0 => updateValue()
      case 1 => removeValue()
      case 2 => addValue()
    }
  }

  val initialSize = 5
  val maximalMutationsPerPropagation = 5

  def run() {

    var mutationCounter = 1

    for(i <- 0 to initialSize)
      addValue()

    val output = mutator.run[Mod[(Int, Int)]](new ListReduceSumTest())
    mutator.propagate()

    while(true) {

      println("m.propagate() // Mutation Cycle " + mutationCounter)
      println("// Current Buffer: " + table.values.toBuffer)

      for(i <- 0 to rand.nextInt(maximalMutationsPerPropagation)) {
        randomMutation()
      }
      mutator.propagate()

      mutationCounter += 1

      val ca = table.values.fold(0)((x:Int, y:Int) => x + y)

      val a = output.read()._2

      println("// Output: " + output.read())

      if(a != ca) {
        println("Check error.")
        println("a: " + a)
        println("ca: " + ca)

        val visualizer = new TbdVisualizer()
        visualizer.showDDG(mutator.getDDG().root)
        readLine()
      }
    }

    mutator.shutdown()
  }
}

class ListReduceSumTest extends Adjustable {
  def run(tbd: TBD): Mod[(Int, Int)] = {
    val modList = tbd.input.getAdjustableList[Int, Int](partitions = 1)
    val zero = tbd.mod((dest : Dest[(Int, Int)]) => tbd.write(dest, (0, 0)))
    modList.reduce(tbd, zero,
      (tbd: TBD, pair1: (Int, Int), pair2: (Int, Int)) => {
        (pair2._1, pair1._2 + pair2._2)
      })
  }
}