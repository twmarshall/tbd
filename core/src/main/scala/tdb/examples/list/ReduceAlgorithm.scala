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
package tdb.examples.list

import java.io._
import scala.collection.GenIterable
import scala.collection.immutable.HashMap
import scala.collection.mutable.Map

import tdb._
import tdb.list._
import tdb.TDB._
import tdb.util._

class ReduceAdjust
    (list: AdjustableList[Int, Int], conf: ListConf)
      extends Adjustable[Mod[(Int, Int)]] {

  def reducer
      (pair1: (Int, Int),
       pair2: (Int, Int)) = {
    (pair1._1, pair1._2 + pair2._2)
  }

  def run(implicit c: Context) = {
    list.reduce(reducer)
  }
}

class ReduceAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[Int, Mod[(Int, Int)]](_conf) {

  var data: Data[Int] = null

  var input: Dataset[Int, Int] = null

  var adjust: ReduceAdjust = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    input = mutator.createList[Int, Int](conf.listConf)
      .asInstanceOf[Dataset[Int, Int]]

    val mappedConf = ListConf(chunkSize = conf.listConf.chunkSize,
      partitions = conf.listConf.partitions)
    adjust = new ReduceAdjust(input.getAdjustableList(), mappedConf)

    data = new IntData(
      input, conf.runs, conf.count, conf.mutations)
    data.generate()
    data.load()
  }

  def checkOutput(output: Mod[(Int, Int)]) = {
    val answer = data.table.values.reduce(_ + _)
    val out = mutator.read(output)._2
    answer == out
  }
}
