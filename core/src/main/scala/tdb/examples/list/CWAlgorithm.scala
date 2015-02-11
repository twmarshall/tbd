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
    (list: AdjustableList[String, String], mappedPartitions: Int)
      extends Adjustable[Iterable[Mod[(Int, Int)]]] {

  def wordcount(chunk: Iterable[(String, String)]) = {
    val counts = Map[Int, Int]()

    for (i <- 0 until mappedPartitions) {
      counts(i) = 0
    }

    for (pair <- chunk) {
      for (word <- pair._2.split("\\W+")) {
        val hash = word.hashCode().abs % mappedPartitions
        counts(hash) += 1
      }
    }

    counts
  }

  def reducer
      (pair1: (Int, Int),
       pair2: (Int, Int)) = {
    (pair1._1, pair1._2 + pair2._2)
  }

  var mapped: AdjustableList[Int, Int] = _
  def run(implicit c: Context) = {
    mapped = list.hashChunkMap(wordcount)
    mapped.partitionedReduce(reducer)
  }
}

class CWChunkHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Iterable[Mod[(Int, Int)]]](_conf) {
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

    adjust = new CWChunkHashAdjust(input.getAdjustableList(), 4)

    data = new FileData(
      mutator, input, conf.file, conf.updateFile, conf.runs)
  }

  def checkOutput(output: Iterable[Mod[(Int, Int)]]) = {
    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("wc-output.txt"), "utf-8"))

    var total = 0
    for (mod <- output) {
      val pair = mutator.read(mod)
      total += pair._2
    }
    writer.write(total + "\n")
    writer.close()

    true
  }
}
