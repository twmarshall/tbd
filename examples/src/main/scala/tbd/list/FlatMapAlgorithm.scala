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

import tbd._
import tbd.datastore.IntData
import tbd.list._

object FlatMapAlgorithm {
  def mapper(pair: (Int, Int)): List[(Int, Int)] = {
    (pair._1, pair._2) :: (pair._1, pair._2 * 2) :: Nil
  }
}

class FlatMapAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Int, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = ListInput[Int, Int](mutator, listConf)

  val data = new IntData(input, count, mutations)

  var naiveTable: GenIterable[Int] = _
  def generateNaive() {
    data.generate()
    naiveTable = Vector(data.table.values.toSeq: _*).par
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[Int]) = {
    input.flatMap(FlatMapAlgorithm.mapper(0, _))
  }

  def checkOutput(table: Map[Int, Int], output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer(mutator).map(_._2).sortWith(_ < _)
    val answer = naiveHelper(table.values)

    sortedOutput == answer.map(_._2).toBuffer.sortWith(_ < _)
  }

  def mapper(pair: (Int, Int)) = {
    mapCount += 1
    FlatMapAlgorithm.mapper(pair)
  }

  def run(implicit c: Context) = {
    val pages = input.getAdjustableList()
    pages.flatMap(mapper)
  }
}
