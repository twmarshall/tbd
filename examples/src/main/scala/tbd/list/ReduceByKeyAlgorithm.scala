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
import scala.collection.mutable.{Buffer, Map}

import tbd._
import tbd.datastore.IntData
import tbd.list._

class ReduceByKeyAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[Int, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = ListInput[Int, Int](mutator, listConf)

  val data = new IntData(input, count, mutations)

  def generateNaive() {
    data.generate()
  }

  def runNaive() {
    naiveHelper(data.table)
  }

  private def naiveHelper(input: Map[Int, Int]) = {
    val mapped = Buffer[(Int, Int)]()
    input.foreach(pair => mapped ++= mapper(pair))

    val reduced = Map[Int, Int]()
    for ((key, value) <- mapped) {
      reduced(key) = value + reduced.getOrElse(key, 0)
    }

    reduced
  }

  def checkOutput(table: Map[Int, Int], output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer().sortWith(_._1 < _._1)
    val answer = naiveHelper(table)
    val sortedAnswer = answer.toBuffer.sortWith(_._1 < _._1)

    sortedOutput == sortedAnswer
  }

  def mapper(pair: (Int, Int)) = {
    mapCount += 1
    List(pair, (pair._1 * 2, pair._2), (pair._1 * 3, pair._2))
  }

  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    val mapped = list.flatMap(mapper)
    mapped.reduceByKey(_ + _)
  }
}
