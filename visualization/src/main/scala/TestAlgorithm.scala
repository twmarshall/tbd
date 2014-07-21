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
import scala.collection.mutable.Map
import tbd._
import tbd.mod.{AdjustableList, Dest, Mod, DoubleModList}

//TODO: Remove TestAlgorithm and make visualizer work with Examples.
trait TestAlgorithm[T, V] extends Adjustable {
  def getResult(output: T): V
  def getExpectedResult(input: Map[Int, Int]): V
  def getListConf() = { new ListConf(partitions = 1) }

  var input: ListInput[Int, Int] = null
}

class ListReduceSumTest()
    extends TestAlgorithm[Mod[(Int, Int)], Int] {

  def run(tbd: TBD): Mod[(Int, Int)] = {
    val modList = input.getAdjustableList()
    val zero = tbd.mod((dest : Dest[(Int, Int)]) => tbd.write(dest, (0, 0)))
    modList.reduce(tbd, zero,
      (tbd: TBD, pair1: (Int, Int), pair2: (Int, Int)) => {
        (pair2._1, pair1._2 + pair2._2)
      })
  }

  def getResult(output: Mod[(Int, Int)]): Int = {
    output.read()._2
  }

  def getExpectedResult(input: Map[Int, Int]): Int = {
    input.values.foldLeft(0)(_ + _)
  }
}

class ListQuicksortTest()
    extends TestAlgorithm[AdjustableList[Int, Int], Seq[Int]] {
  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val modList = input.getAdjustableList()
    modList.sort(tbd, (tbd, a, b) => a._2 < b._2, true, true)
  }

  def getResult(output:  AdjustableList[Int, Int]): Seq[Int] = {
    output.toBuffer()
  }

  def getExpectedResult(input: Map[Int, Int]): Seq[Int] = {
    input.values.toBuffer.sortWith(_ < _)
  }
}

class ListSplitTest()
    extends TestAlgorithm[(AdjustableList[Int, Int], AdjustableList[Int, Int]), (Seq[Int], Seq[Int])] {
  def run(tbd: TBD): (AdjustableList[Int, Int], AdjustableList[Int, Int]) = {
    val modList = input.getAdjustableList()
    modList.split(tbd, (tbd, a) => a._2 % 2 == 0, false, false)
  }

  def getResult(output:  (AdjustableList[Int, Int], AdjustableList[Int, Int])): (Seq[Int], Seq[Int]) = {
    (output._1.toBuffer(), output._2.toBuffer())
  }

  def getExpectedResult(input: Map[Int, Int]): (Seq[Int], Seq[Int]) = {
    (input.values.toBuffer.filter(x => x % 2 == 0), input.values.toBuffer.filter(x => x % 2 != 0))
  }
}

class ListMapTest()
    extends TestAlgorithm[AdjustableList[Int, Int], Seq[Int]] {
  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val modList = input.getAdjustableList()
    modList.map(tbd, (tbd, a) => (a._1, a._2 * 2), false, true)
  }

  def getResult(output: AdjustableList[Int, Int]): Seq[Int] = {
    output.toBuffer().sortWith(_ < _)
  }

  def getExpectedResult(input: Map[Int, Int]): Seq[Int] = {
    input.values.map(x => x * 2).toBuffer.sortWith(_ < _)
  }
}