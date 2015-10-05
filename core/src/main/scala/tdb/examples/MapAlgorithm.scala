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
package tdb.examples

import java.io._
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import scala.concurrent.forkjoin.ForkJoinPool

import tdb._
import tdb.list._
import tdb.util._

object MapAlgorithm {
  def mapper(pair: (String, String)): (String, Int) = {
    var count = 0

    for (word <- pair._2.split("\\W+")) {
      count += 1
    }

    (pair._1, count)
  }
}

class MapAdjust(list: AdjustableList[String, String])
  extends Adjustable[AdjustableList[String, Int]] {
  def run(implicit c: Context) = {
    list.map(MapAlgorithm.mapper)
  }
}

class MapAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[AdjustableList[String, Int]](_conf) {
  val input = mutator.createList[String, String](conf.listConf)

  val adjust = new MapAdjust(input.getAdjustableList())

  val data =
    if (OS.isDir(conf.updateFile)) {
      new DirectoryData(input, conf.file, conf.updateFile, conf.runs,
                        Experiment.check)
    } else {
      new FileData(
        input, conf.file, conf.updateFile, conf.runs, Experiment.check)
    }

  var naiveTable: ParIterable[(String, String)] = _
  def generateNaive() {
    data.generate()
    naiveTable = Vector(data.table.toSeq: _*).par
    naiveTable.tasksupport =
      new ForkJoinTaskSupport(new ForkJoinPool(OS.getNumCores() * 2))
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[(String, String)]) = {
    input.map(MapAlgorithm.mapper)
  }

  def loadInitial() {
    data.load()
  }

  def hasUpdates() = data.hasUpdates()

  def loadUpdate() = data.update()

  def checkOutput(output: AdjustableList[String, Int]) = {
    val answer = naiveHelper(data.table)

    val sortedAnswer = answer.toBuffer.sortWith(_._1 < _._1)
    val sortedOutput = output.toBuffer(mutator).sortWith(_._1 < _._1)

    sortedAnswer == sortedOutput
  }
}
