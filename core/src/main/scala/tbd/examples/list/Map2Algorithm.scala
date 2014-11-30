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
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import scala.concurrent.forkjoin.ForkJoinPool

import tbd._
import tbd.datastore.StringData
import tbd.list._

object Map2Algorithm {
  def mapper(pair: (Int, String)): ((Int, Int), (Int, Int)) = {
    var count = 0
    var count2 = 0
    for (word <- pair._2.split("\\W+")) {
      if (word.hashCode() % 2 == 0) {
        count += 1
      } else {
        count2 += 1
      }
    }
    ((pair._1, count), (pair._1, count2))
  }
}

class Map2Adjust(list: AdjustableList[Int, String])
  extends Adjustable[(AdjustableList[Int, Int], AdjustableList[Int, Int])] {
  def run(implicit c: Context) = {
    list.map2(Map2Algorithm.mapper)
  }
}

class Map2Algorithm(_conf: Map[String, _], _listConf: ListConf)
    extends Algorithm[String, (AdjustableList[Int, Int], AdjustableList[Int, Int])](_conf, _listConf) {
  val input = mutator.createList[Int, String](listConf)

  val adjust = new Map2Adjust(input.getAdjustableList())

  val data = new StringData(input, count, mutations, Experiment.check)

  var naiveTable: ParIterable[String] = _
  def generateNaive() {
    data.generate()
    naiveTable = Vector(data.table.values.toSeq: _*).par
    naiveTable.tasksupport =
      new ForkJoinTaskSupport(new ForkJoinPool(partitions * 2))
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[String]) = {
    val buf1 = Buffer[(Int, Int)]()
    val buf2 = Buffer[(Int, Int)]()
    input.foreach {
      v => {
        val (one, two) = Map2Algorithm.mapper(0, v)
        buf1 += one
        buf2 += two
      }
    }

    (buf1, buf2)
  }

  def checkOutput
      (table: Map[Int, String],
       output: (AdjustableList[Int, Int], AdjustableList[Int, Int])) = {
    val sortedOutput1 = output._1.toBuffer(mutator).map(_._2).sortWith(_ < _)
    val sortedOutput2 = output._2.toBuffer(mutator).map(_._2).sortWith(_ < _)
    val (answer1, answer2) = naiveHelper(table.values)

    sortedOutput1 == answer1.map(_._2).toBuffer.sortWith(_ < _) &&
    sortedOutput2 == answer2.map(_._2).toBuffer.sortWith(_ < _)
  }
}
