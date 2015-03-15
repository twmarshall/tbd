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
    extends Algorithm[String, AdjustableList[String, Int]](_conf) {
  var input: ListInput[String, String] = null

  var adjust: MapAdjust = null

  var data: FileData = null

  var naiveTable: ParIterable[String] = _
  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    input = mutator.createList[String, String](conf.listConf)

    adjust = new MapAdjust(input.getAdjustableList())

    data = new FileData(
      mutator, input, conf.file, conf.updateFile, conf.runs)
  }

  def checkOutput(output: AdjustableList[String, Int]) = {
    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("map-output.txt"), "utf-8"))

    for ((key, value) <- output.toBuffer(mutator).sortWith(_._1 < _._1)) {
      writer.write(key + " -> " + value + "\n")
    }
    writer.close()

    true
  }
}
