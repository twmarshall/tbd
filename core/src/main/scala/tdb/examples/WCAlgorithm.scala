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

import scala.collection.{GenIterable, GenMap}
import scala.collection.immutable.HashMap
import scala.collection.mutable.Map
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import scala.concurrent.forkjoin.ForkJoinPool

import tdb._
import tdb.list._
import tdb.TDB._
import tdb.util._

object WCAlgorithm {
  def wordcount(s: String): HashMap[String, Int] = {
    HashMap(mutableWordcount(s).toSeq: _*)
  }

  def mutableWordcount(s: String, counts: Map[String, Int] = Map[String, Int]())
      : Map[String, Int] = {
    for (word <- s.split("\\W+")) {
      if (counts.contains(word)) {
        counts(word) += 1
      } else {
        counts(word) = 1
      }
    }

    counts
  }

  def countReduce(s: String, counts: Map[String, Int]): Map[String, Int] = {
    for (word <- s.split("\\W+")) {
      if (counts.contains(word)) {
        counts(word) += 1
      } else {
        counts(word) = 1
      }
    }

    counts
  }

  def reduce(map1: HashMap[String, Int], map2: HashMap[String, Int])
      : HashMap[String, Int] = {
    map1.merged(map2)({ case ((k, v1),(_, v2)) => (k, v1 + v2)})
  }

  def mutableReduce(map1: Map[String, Int], map2: Map[String, Int])
      : Map[String, Int] = {
    val counts = map2.clone()
    for ((key, value) <- map1) {
      if (counts.contains(key)) {
        counts(key) += map1(key)
      } else {
        counts(key) = map1(key)
      }
    }
    counts
  }
}

class WCAdjust(list: AdjustableList[String, String])
  extends Adjustable[Mod[(String, HashMap[String, Int])]] {

  def run(implicit c: Context) = {
    val counts = list.map {
      case (title, body) => (title, WCAlgorithm.wordcount(body))
    }

    counts.reduce {
      case ((key1, value1), (key2, value2)) =>
        (key1, WCAlgorithm.reduce(value1, value2))
    }
  }
}


class ChunkWCAdjust(list: AdjustableList[String, String])
  extends Adjustable[Mod[(String, HashMap[String, Int])]] {

  def chunkMapper(chunk: Iterable[(String, String)]) = {
    var counts = Map[String, Int]()

    for (page <- chunk) {
      counts = WCAlgorithm.mutableWordcount(page._2, counts)
    }

    (chunk.head._1, HashMap(counts.toSeq: _*))
  }

  def run(implicit c: Context) = {
    val counts = list.chunkMap(chunkMapper)
    counts.reduce {
      case ((key1, value1), (key2, value2)) =>
        (key1, WCAlgorithm.reduce(value1, value2))
    }
  }
}

class WCAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[Mod[(String, HashMap[String, Int])]](_conf) {

  val input = mutator.createList[String, String](conf.listConf)

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

  val adjust =
    if (conf.listConf.chunkSize == 1)
      new WCAdjust(input.getAdjustableList())
    else
      new ChunkWCAdjust(input.getAdjustableList())

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

  def loadInitial() {
    data.load()
  }

  def hasUpdates() = data.hasUpdates()

  def loadUpdate() = data.update()

  def checkOutput(output: Mod[(String, HashMap[String, Int])]) = {
    val answer = naiveHelper(data.table.values)
    val out = mutator.read(output)._2

    //println("answer = " + answer)
    //println("output = " + out)

    out == answer
  }
}
