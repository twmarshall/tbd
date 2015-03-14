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

  def reducer
      (pair1: (Int, HashMap[String, Int]),
       pair2: (Int, HashMap[String, Int])) = {
    val reduced =
      pair1._2.merged(pair2._2)({ case ((k, v1),(_, v2)) => (k, v1 + v2)})
    (pair1._1, reduced)
  }

  def run(implicit c: Context) = {
    val conf = AggregatorListConf(
      aggregator = (_: Int) + (_: Int),
      deaggregator = (_: Int) + (_: Int),
      initialValue = 0,
      threshold = (_: Int) > 0)
    list.hashChunkMap(wordcount, conf).getAdjustableList()
  }
}

class WCChunkHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, AdjustableList[String, Int]](_conf) {

  val outputFile = "output"
  if (Experiment.check) {
    val args = Array("-f", conf.file, "--updateFile", conf.updateFile,
                     "-o", outputFile, "-u") ++ conf.runs
    tdb.scripts.NaiveWC.main(args)

  }

  var data: Data[String] = null

  var input: ListInput[String, String] = null

  var adjust: WCChunkHashAdjust = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    mutator.loadFile(conf.file)
    input = mutator.createList[String, String](
      conf.listConf.clone(file = conf.file))

    adjust = new WCChunkHashAdjust(
      input.getAdjustableList(), conf.listConf.chunkSize)

    data = new FileData(
      mutator, input, conf.file, conf.updateFile, conf.runs)
  }

  def checkOutput(output: AdjustableList[String, Int]) = {
    val thisFile = "wc-output" + lastUpdateSize + ".txt"
    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(thisFile), "utf-8"))

    val sortedOutput = output.toBuffer(mutator).sortWith(_._1 < _._1)

    for ((word, count) <- sortedOutput) {
      writer.write(word + " -> " + count + "\n")
    }
    writer.close()

    val thisOutputFile =
      if (lastUpdateSize != 0)
        outputFile + lastUpdateSize + ".txt"
      else
        outputFile + ".txt"

    val f1 = scala.io.Source.fromFile(thisOutputFile)
    val f2 = scala.io.Source.fromFile(thisFile)

    val one = f1.getLines.mkString("\n")
    val two = f2.getLines.mkString("\n")

    assert(one == two)

    true
  }
}


class RandomWCAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, AdjustableList[String, Int]](_conf) {

  var data: Data[String] = null

  var input: ListInput[String, String] = null

  var adjust: WCChunkHashAdjust = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    input = mutator.createList[String, String](conf.listConf)

    adjust = new WCChunkHashAdjust(
      input.getAdjustableList(), conf.listConf.chunkSize)
    data = new RandomStringData(
      input, conf.count, conf.mutations, Experiment.check, conf.runs)
    data.generate()
    data.load()
  }

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
