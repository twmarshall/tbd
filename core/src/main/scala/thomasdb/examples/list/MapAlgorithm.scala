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
package thomasdb.examples.list

import java.io._
import scala.collection.{GenIterable, GenMap, Seq}
import scala.collection.mutable.Map
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import scala.concurrent.forkjoin.ForkJoinPool

import thomasdb._
import thomasdb.list._
import thomasdb.util._

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
  var adjust: MapAdjust = null

  val data = new DummyData()
  //val data = new StringFileData(input, conf.file)

  var naiveTable: ParIterable[String] = _
  def generateNaive() {
    /*data.generate()
    naiveTable = Vector(data.table.values.toSeq: _*).par
    naiveTable.tasksupport =
      new ForkJoinTaskSupport(new ForkJoinPool(conf.listConf.partitions * 2))*/
  }

  def runNaive() {
    //naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[(String, String)]) = {
    input.map(MapAlgorithm.mapper)
  }

  var input: ListInput[String, String] = null
  override def loadInitial() {
    input =
      mutator.createList[String, String](
        conf.listConf.copy(file = conf.file))
    adjust = new MapAdjust(input.getAdjustableList())
  }

  def checkOutput(output: AdjustableList[String, Int]) = {
    /*val sortedOutput = output.toBuffer(mutator).sortWith(_._1 < _._1)
    val sortedAnswer =
      naiveHelper(input.getAdjustableList.toBuffer(mutator))
        .toBuffer.sortWith(_._1 < _._1)

    if (Experiment.verbosity > 1) {
      println(sortedOutput)
      println(sortedAnswer)
    }

    sortedOutput == sortedAnswer*/

    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("map-output.txt"), "utf-8"))

    for ((key, value) <- output.toBuffer(mutator).sortWith(_._1 < _._1)) {
      writer.write(key + " -> " + value + "\n")
    }
    writer.close()

    true
  }
}
