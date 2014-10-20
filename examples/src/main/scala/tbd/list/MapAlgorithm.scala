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
import scala.concurrent.{Await, Future}

import tbd._
import tbd.Constants._
import tbd.datastore.StringData
import tbd.list._

object MapAlgorithm {
  def mapper(pair: (Int, String)): (Int, Int) = {
    var count = 0
    for (word <- pair._2.split("\\W+")) {
      count += 1
    }
    (pair._1, count)
  }
}

class MapAlgorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, AdjustableList[Int, Int]](_conf, _listConf) {
  import scala.concurrent.ExecutionContext.Implicits.global

  val input = ListInput[Int, String](mutator, listConf)

  val data = new StringData(input, count, mutations, Experiment.check)

  //var naiveTable: GenIterable[String] = _
  var naiveTable = Buffer[GenIterable[String]]()
  def generateNaive() {
    data.generate()
    var remaining = data.table.values
    for (i <- 1 to partitions) {
      naiveTable += remaining.take(data.table.size / partitions)
      remaining = remaining.takeRight(data.table.size / partitions)
    }
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper
      (input: Buffer[GenIterable[String]]): Buffer[GenIterable[Int]] = {
    val futures = Buffer[Future[GenIterable[Int]]]()
    for (partition <- input) {
      futures += Future {
	partition.map(MapAlgorithm.mapper(0, _)._2)
      }
    }
    Await.result(Future.sequence(futures), DURATION)
  }

  def checkOutput(table: Map[Int, String], output: AdjustableList[Int, Int]) = {
    val sortedOutput = output.toBuffer(mutator).map(_._2).sortWith(_ < _)
    val answer = naiveHelper(Buffer(table.values))
    val sortedAnswer = answer.reduce(_ ++ _).toBuffer.sortWith(_ < _)

    //println(sortedOutput)
    //println(sortedAnswer)
    sortedOutput == sortedAnswer
  }

  def mapper(pair: (Int, String)) = {
    mapCount += 1
    MapAlgorithm.mapper(pair)
  }

  def run(implicit c: Context) = {
    val pages = input.getAdjustableList()
    pages.map(mapper)
  }
}
