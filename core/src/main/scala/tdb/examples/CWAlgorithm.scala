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

class CWAdjust
    (list: AdjustableList[String, String], conf: ListConf)
      extends Adjustable[Mod[(String, Int)]] {

  def run(implicit c: Context) = {
    val mapped = createList[String, Int](conf)
    list.foreach {
      case ((title, page), c) =>
        put(mapped, title, page.split("\\W+").size)(c)
    }

    mapped.getAdjustableList().reduce {
      case ((key1, value1), (key2, value2)) => (key1, value1 + value2)
    }
  }
}

class CWChunkAdjust
    (list: AdjustableList[String, String], conf: ListConf)
      extends Adjustable[Mod[(String, Int)]] {

  def run(implicit c: Context) = {
    val mapped = createList[String, Int](conf)
    list.foreachChunk {
      case (values, c) =>
        putAll(mapped, values.map {
          case (title, page) => (title, page.split("\\W+").size)
        })(c)
    }

    mapped.getAdjustableList().reduce {
      case ((key1, value1), (key2, value2)) =>
        (key1, value1 + value2)
    }
  }
}

class CWChunkHashAlgorithm(_conf: AlgorithmConf)
  extends Algorithm[String, Mod[(String, Int)]](_conf) {

  val outputFile = "output"
  if (Experiment.check) {
    val args = Array("-f", conf.file, "--updateFile", conf.updateFile,
                     "-o", outputFile, "-u") ++ conf.runs
    tdb.scripts.NaiveCW.main(args)

  }

  var data: FileData = null

  var input: ListInput[String, String] = null

  var adjust: Adjustable[Mod[(String, Int)]] = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    mutator.loadFile(conf.file)
    input = mutator.createList[String, String](
        conf.listConf.clone(file = conf.file))

    val mappedConf = ListConf(chunkSize = conf.listConf.chunkSize,
      partitions = conf.listConf.partitions)
    adjust =
      if (conf.listConf.chunkSize == 1)
        new CWAdjust(input.getAdjustableList(), mappedConf)
      else
        new CWChunkAdjust(input.getAdjustableList(), mappedConf)

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

  var input: ListInput[String, String] = null

  var adjust: Adjustable[Mod[(String, Int)]] = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    input = mutator.createList[String, String](conf.listConf)

    val mappedConf = ListConf(chunkSize = conf.listConf.chunkSize,
      partitions = conf.listConf.partitions)
    adjust =
      if (conf.listConf.chunkSize == 1)
        new CWAdjust(input.getAdjustableList(), mappedConf)
      else
        new CWChunkAdjust(input.getAdjustableList(), mappedConf)

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
