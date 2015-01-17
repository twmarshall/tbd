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
import scala.collection.{GenIterable, GenMap}
import scala.collection.immutable.HashMap
import scala.collection.mutable.Map
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import scala.concurrent.forkjoin.ForkJoinPool

import thomasdb._
import thomasdb.list._
import thomasdb.ThomasDB._
import thomasdb.util._

class WCHashAdjust(list: AdjustableList[String, String], mappedPartitions: Int)
    extends Adjustable[Iterable[Mod[(Int, HashMap[String, Int])]]] {

  def wordcount(pair: (String, String)) = {
    val counts = Map[Int, Map[String, Int]]()

    for (i <- 0 until mappedPartitions) {
      counts(i) = Map[String, Int]()
    }

    for (word <- pair._2.split("\\W+")) {
      val hash = word.hashCode().abs % mappedPartitions
      if (counts(hash).contains(word)) {
        counts(hash)(word) += 1
      } else {
        counts(hash)(word) = 1
      }
    }

    counts.map((pair: (Int, Map[String, Int])) => {
      (pair._1, HashMap(pair._2.toSeq: _*))
    })
  }

  def reducer
      (pair1: (Int, HashMap[String, Int]),
       pair2: (Int, HashMap[String, Int])) = {
    val reduced = reduce(pair1._2, pair2._2)
    (pair1._1, reduced)
  }

  def reduce(map1: HashMap[String, Int], map2: HashMap[String, Int]) = {
    map1.merged(map2)({ case ((k, v1),(_, v2)) => (k, v1 + v2)})
  }

  var mapped: AdjustableList[Int, HashMap[String, Int]] = _
  def run(implicit c: Context) = {
    mapped = list.hashPartitionedFlatMap(wordcount, mappedPartitions)
    mapped.partitionedReduce(reducer)
  }
}

class WCHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Iterable[Mod[(Int, HashMap[String, Int])]]](_conf) {
  //val input = mutator.createList[Int, String](conf.listConf.copy(double = true))

  //val data = new StringData(input, conf.count, conf.mutations, Experiment.check, conf.runs)
  //val data = new StringFileData(input, "data.txt")
  val data = new DummyData()

  var adjust: WCHashAdjust = null
    //new WCHashAdjust(input.getAdjustableList(), conf.listConf.partitions)

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

  private def naiveHelper(input: GenIterable[String] = naiveTable) = {
    input.aggregate(Map[String, Int]())((x, line) =>
      WCAlgorithm.countReduce(line, x), WCAlgorithm.mutableReduce)
  }

  var input: ListInput[String, String] = null
  override def loadInitial() {
    input =
      mutator.createList[String, String](
        conf.listConf.copy(file = conf.file, double = true))
    adjust = new WCHashAdjust(input.getAdjustableList(), 2)
  }

  def checkOutput(output: Iterable[Mod[(Int, HashMap[String, Int])]]) = {
    /*val answer = naiveHelper(data.table.values)
    var out = HashMap[String, Int]()

    for (mod <- output) {
      val value = mutator.read(mod)
      if (value != null) {
        out = out ++ value._2
      }
    }

    out == answer*/

    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("wc-output.txt"), "utf-8"))

    val sortedOutput = output.toBuffer.map((x: Mod[(Int, HashMap[String, Int])]) => {
      mutator.read(x)
    }).sortWith(_._1 < _._1)

    for ((key, hashMap) <- sortedOutput) {
      for ((word, count) <- hashMap.toBuffer.sortWith(_._1 < _._1)) {
        writer.write(word + " -> " + count + "\n")
      }
    }
    writer.close()

    true
  }
}

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

  def mapper(pair: (Int, String)) = {
    (pair._1, wordcount(pair._2))
  }

  def reducer
      (pair1: (Int, HashMap[String, Int]),
       pair2: (Int, HashMap[String, Int])) = {
    val reduced = reduce(pair1._2, pair2._2)
    (pair1._1, reduced)
  }
}

class WCAdjust(list: AdjustableList[Int, String])
  extends Adjustable[Mod[(Int, HashMap[String, Int])]] {

  def run(implicit c: Context): Mod[(Int, HashMap[String, Int])] = {
    val counts = list.map(WCAlgorithm.mapper)
    counts.reduce(WCAlgorithm.reducer)
  }
}

class WCAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Mod[(Int, HashMap[String, Int])]](_conf) {
  val input = mutator.createList[Int, String](conf.listConf.copy(double = true))

  val data = new StringData(input, conf.count, conf.mutations, Experiment.check, conf.runs)
  //val data = new StringFileData(input, "data.txt")

  val adjust = new WCAdjust(input.getAdjustableList())

  var naiveTable: ParIterable[String] = _
  def generateNaive() {
    data.generate()
    naiveTable = Vector(data.table.values.toSeq: _*).par
    naiveTable.tasksupport =
      new ForkJoinTaskSupport(new ForkJoinPool(conf.listConf.partitions * 2))
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[String] = naiveTable) = {
    input.aggregate(Map[String, Int]())((x, line) =>
      WCAlgorithm.countReduce(line, x), WCAlgorithm.mutableReduce)
  }

  def checkOutput(output: Mod[(Int, HashMap[String, Int])]) = {
    val answer = naiveHelper(data.table.values)
    val out = mutator.read(output)._2

    out == answer
  }
}

class ChunkWCAdjust(list: AdjustableList[Int, String])
  extends Adjustable[Mod[(Int, HashMap[String, Int])]] {

  def chunkMapper(chunk: Vector[(Int, String)]) = {
    var counts = Map[String, Int]()

    for (page <- chunk) {
      counts = WCAlgorithm.mutableWordcount(page._2, counts)
    }

    (0, HashMap(counts.toSeq: _*))
  }

  def chunkReducer(
      pair1: (Int, HashMap[String, Int]),
      pair2: (Int, HashMap[String, Int])) = {
    (pair1._1, WCAlgorithm.reduce(pair1._2, pair2._2))
  }

  def run(implicit c: Context): Mod[(Int, HashMap[String, Int])] = {
    val counts = list.chunkMap(chunkMapper)
    counts.reduce(chunkReducer)
  }
}

class ChunkWCAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Mod[(Int, HashMap[String, Int])]](_conf) {
  val input = mutator.createList[Int, String](conf.listConf)

  val data = new StringData(input, conf.count, conf.mutations, Experiment.check, conf.runs)

  val adjust = new ChunkWCAdjust(input.getAdjustableList())

  var naiveTable: ParIterable[String] = _
  def generateNaive() {
    data.generate()
    naiveTable = Vector(data.table.values.toSeq: _*).par
    naiveTable.tasksupport =
      new ForkJoinTaskSupport(new ForkJoinPool(conf.listConf.partitions * 2))
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(input: GenIterable[String] = naiveTable) = {
    input.aggregate(Map[String, Int]())((x, line) =>
      WCAlgorithm.countReduce(line, x), WCAlgorithm.mutableReduce)
  }

  def checkOutput(output: Mod[(Int, HashMap[String, Int])]) = {
    val answer = naiveHelper(data.table.values)
    mutator.read(output)._2 == answer
  }
}