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
import scala.collection.GenIterable
import scala.collection.immutable.HashMap
import scala.collection.mutable.Map
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import scala.concurrent.forkjoin.ForkJoinPool

import tdb._
import tdb.list._
import tdb.TDB._
import tdb.util._

class WCChunkHashAdjust
    (list: AdjustableList[String, String], chunkSize: Int)
      extends Adjustable[AdjustableList[String, Int]] {

  def wordcount(chunk: Iterable[(String, String)]) = {
    val counts = Map[String, Int]()

    for (pair <- chunk) {
      for (word <- pair._2.split("\\W+")) {
        if (counts.contains(word)) {
          counts(word) += 1
        } else {
          counts(word) = 1
        }
      }
    }

    HashMap(counts.toSeq: _*)
  }

  def run(implicit c: Context) = {
    val conf = AggregatorListConf(
      valueType = AggregatedIntColumn())

    val output = createList[String, Int](conf)
    list.foreach {
      case (chunk, c) =>
        putAll(output, wordcount(Iterable(chunk)))(c)
    }
    flush(output)
    output.getAdjustableList()
  }
}

class WCHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[AdjustableList[String, Int]](_conf) {

  val input = mutator.createList[String, String](conf.listConf)

  val adjust = new WCChunkHashAdjust(
    input.getAdjustableList(), conf.listConf.chunkSize)

  val data =
    if (conf.file == "") {
      if (Experiment.verbosity > 0) {
        println("Generating random data.")
      }
      new RandomStringData(
        input, conf.count, conf.mutations, Experiment.check, conf.runs)
    } else {
      if (Experiment.verbosity > 0) {
        println("Reading data from " + conf.file)
      }
      new FileData(
        input, conf.file, conf.updateFile, conf.runs)
    }

  var naiveTable: ParIterable[String] = _
  def generateNaive() {
    data.generate()
    naiveTable = Vector(data.table.values.toSeq: _*).par
    naiveTable.tasksupport =
      new ForkJoinTaskSupport(new ForkJoinPool(OS.getNumCores() * 2))
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[String] = naiveTable) = {
    input.aggregate(Map[String, Int]())((x, line) =>
      WCAlgorithm.countReduce(line, x), WCAlgorithm.mutableReduce)
  }

  override def loadInitial() {
    data.load()
  }

  def hasUpdates() = data.hasUpdates()

  def loadUpdate() = data.update()

  def checkOutput(output: AdjustableList[String, Int]) = {
    val answer = Map[String, Int]()
    for ((key, value) <- data.table) {
      for (word <- value.split("\\W+")) {
        if (answer.contains(word)) {
          answer(word) += 1
        } else {
          answer(word) = 1
        }
      }
    }

    val sortedOutput = output.toBuffer(mutator).sortWith(_._1 < _._1)
    val sortedAnswer = answer.toBuffer.sortWith(_._1 < _._1)

    //println("output = " + sortedOutput)
    //println("answer = " + sortedAnswer)

    sortedOutput == sortedAnswer
  }
}