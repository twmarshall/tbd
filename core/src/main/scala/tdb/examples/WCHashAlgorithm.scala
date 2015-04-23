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

object WCHash {
  val wordFile = new BufferedReader(new FileReader("words.txt"))

  var words = Set[String]()
  var line = wordFile.readLine()
  while (line != null) {
    words += line
    line = wordFile.readLine()
  }

  def countReduce(s: String, counts: Map[String, Int]) = {
    for (word <- s.split("\\W+")) {
      if (words.contains(word)) {
        if (counts.contains(word)) {
          counts(word) += 1
        } else {
          counts(word) = 1
        }
      }
    }

    counts
  }
}

class WCHashAdjust
    (list: AdjustableList[String, String], conf: ListConf)
      extends Adjustable[AdjustableList[String, Int]] {

  def wordcount(pair: (String, String)) = {
    val counts = Map[String, Int]()
    //println("mapping " + pair + "\n")
    WCHash.countReduce(pair._2, counts)

    HashMap(counts.toSeq: _*)
  }

  def run(implicit c: Context) = {
    val aggConf = AggregatorListConf(
      partitions = conf.partitions,
      valueType = AggregatedIntColumn())

    val output = createList[String, Int](aggConf)
    list.foreach {
      case (chunk, c) =>
        putAll(output, wordcount(chunk))(c)
    }

    output.getAdjustableList()
  }
}


class WCChunkHashAdjust
    (list: AdjustableList[String, String], conf: ListConf)
      extends Adjustable[AdjustableList[String, Int]] {

  def wordcount(chunk: Iterable[(String, String)]) = {
    val counts = Map[String, Int]()

    for (pair <- chunk) {
      WCHash.countReduce(pair._2, counts)
    }

    HashMap(counts.toSeq: _*)
  }

  def run(implicit c: Context) = {
    val aggConf = AggregatorListConf(
      partitions = conf.partitions,
      valueType = AggregatedIntColumn())

    val output = createList[String, Int](aggConf)
    list.foreachChunk {
      case (chunk, c) =>
        putAll(output, wordcount(chunk))(c)
    }

    output.getAdjustableList()
  }
}

class WCHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[AdjustableList[String, Int]](_conf) {

  val input = mutator.createList[String, String](conf.listConf)

  val adjust =
    if (conf.listConf.chunkSize > 1)
      new WCChunkHashAdjust(input.getAdjustableList(), conf.listConf)
    else
      new WCHashAdjust(input.getAdjustableList(), conf.listConf)

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

      if (OS.isDir(conf.updateFile)) {
        new DirectoryData(input, conf.file, conf.updateFile, conf.runs)
      } else {
        new FileData(input, conf.file, conf.updateFile, conf.runs)
      }
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
      WCHash.countReduce(line, x), WCAlgorithm.mutableReduce)
  }

  override def loadInitial() {
    data.load()
  }

  def hasUpdates() = data.hasUpdates()

  def loadUpdate() = data.update()

  def checkOutput(output: AdjustableList[String, Int]) = {
    val answer = naiveHelper(data.table.values)

    val sortedOutput = output.toBuffer(mutator).sortWith(_._1 < _._1)
    val sortedAnswer = answer.toBuffer.sortWith(_._1 < _._1)

    //println("output = " + sortedOutput)
    //println("answer = " + sortedAnswer)

    sortedOutput == sortedAnswer
  }
}
