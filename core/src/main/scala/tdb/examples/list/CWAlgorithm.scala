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

class CWChunkHashAdjust
    (list: AdjustableList[String, String], conf: ListConf)
      extends Adjustable[Mod[(String, Int)]] {

  def wordcount(chunk: Iterable[(String, String)]) = {
    val counts = Map[String, Int]()

    for (pair <- chunk) {
      counts(pair._1) = pair._2.split("\\W+").size
    }

    counts
  }

  def reducer
      (pair1: (String, Int),
       pair2: (String, Int)) = {
    (pair1._1, pair1._2 + pair2._2)
  }

  def run(implicit c: Context) = {
    val mapped = list.hashChunkMap(wordcount, conf).getAdjustableList()
    mapped.reduce(reducer)
  }
}

class CWChunkHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Mod[(String, Int)]](_conf) {
    //extends Algorithm[String, AdjustableList[String, Int]](_conf) {

  val outputFile = "output"
  if (Experiment.check) {
    val args = Array("-f", conf.file, "--updateFile", conf.updateFile,
                     "-o", outputFile, "-u") ++ conf.runs
    tdb.scripts.NaiveCW.main(args)

  }

  var data: FileData = null

  var input: Dataset[String, String] = null

  var adjust: CWChunkHashAdjust = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    mutator.loadFile(conf.file)
    input = mutator.createList[String, String](
        conf.listConf.copy(file = conf.file))
          .asInstanceOf[Dataset[String, String]]

    val mappedConf = ListConf(chunkSize = conf.listConf.chunkSize,
      partitions = conf.listConf.partitions)
    adjust = new CWChunkHashAdjust(input.getAdjustableList(), mappedConf)

    data = new FileData(
      mutator, input, conf.file, conf.updateFile, conf.runs)
  }

  def checkOutput(output: Mod[(String, Int)]) = {
    val thisFile = "wc-output" + lastUpdateSize + ".txt"
    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(thisFile), "utf-8"))

    val pair = mutator.read(output)
    writer.write(pair._2 + "\n")

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

class RandomCWAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Mod[(String, Int)]](_conf) {

  var data: Data[String] = null

  var input: Dataset[String, String] = null

  var adjust: CWChunkHashAdjust = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    input = mutator.createList[String, String](conf.listConf)
      .asInstanceOf[Dataset[String, String]]

    val mappedConf = ListConf(chunkSize = conf.listConf.chunkSize,
      partitions = conf.listConf.partitions)
    adjust = new CWChunkHashAdjust(input.getAdjustableList(), mappedConf)

    data = new RandomStringData(
      input, conf.count, conf.mutations, Experiment.check, conf.runs)
    data.generate()
    data.load()
  }

  def checkOutput(output: Mod[(String, Int)]) = {
    var answer = 0
    for ((key, value) <- data.table) {
      answer += value.split("\\W+").size
    }

    val out = mutator.read(output)._2

    //println("answer = " + answer)
    //println("output = " + out)

    answer == out
  }
}
