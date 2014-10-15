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

import scala.collection.{GenIterable, GenMap}
import scala.collection.mutable.Map
import scala.collection.immutable.HashMap

import tbd._
import tbd.datastore.StringData
import tbd.list._
import tbd.TBD._

class WikiSortAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, AdjustableList[Int, Int]](_conf, _listConf) {
  val input = ListInput[Int, String](mutator, listConf)

  val data = new StringData(input, count, mutations, Experiment.check)

  def generateNaive() {
    data.generate()
  }

  def runNaive() {
    naiveHelper(data.table)
  }

  private def naiveHelper(input: Map[Int, String]) = {
    input.toBuffer.map(mapper)
  }

  def checkOutput
      (table: Map[Int, String],
       output: AdjustableList[Int, Int]) = {
    val answer = naiveHelper(table)

    //println(output.toBuffer)
    //println(answer.toBuffer)

    output.toBuffer(mutator) == answer.toBuffer.sortWith(_._1 < _._1)
  }

  def mapper(pair: (Int, String)) = {
    (pair._2.split("\\W+").size, pair._1)
  }

  def run(implicit c: Context) = {
    val pages = input.getAdjustableList()
    val counts = pages.map(mapper)
    counts.quicksort()
  }
}
