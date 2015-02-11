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
    val reduced =
      pair1._2.merged(pair2._2)({ case ((k, v1),(_, v2)) => (k, v1 + v2)})
    (pair1._1, reduced)
  }

  var mapped: AdjustableList[Int, HashMap[String, Int]] = _
  def run(implicit c: Context) = {
    mapped = list.hashPartitionedFlatMap(wordcount, mappedPartitions)
    mapped.partitionedReduce(reducer)
  }
}

class WCHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Iterable[Mod[(Int, HashMap[String, Int])]]](_conf) {
  var data: FileData = null

  var input: Dataset[String, String] = null

  var adjust: WCHashAdjust = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    mutator.loadFile(conf.file)
    input = mutator.createList[String, String](
        conf.listConf.copy(file = conf.file))
          .asInstanceOf[Dataset[String, String]]

    adjust = new WCHashAdjust(input.getAdjustableList(), 4)

    data = new FileData(
      mutator, input, conf.file, conf.updateFile, conf.runs)
  }

  def checkOutput(output: Iterable[Mod[(Int, HashMap[String, Int])]]) = {
    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("wc-output.txt"), "utf-8"))

    val sortedOutput = output.toBuffer.flatMap(
      (x: Mod[(Int, HashMap[String, Int])]) => {
        val v = mutator.read(x)

        if (v == null) {
          List()
        } else {
          v._2
        }
      }).sortWith(_._1 < _._1)

    for ((word, count) <- sortedOutput) {
      writer.write(word + " -> " + count + "\n")
    }
    writer.close()

    true
  }
}


class WCChunkHashAdjust
    (list: AdjustableList[String, String], mappedPartitions: Int)
      extends Adjustable[Iterable[Mod[(Int, HashMap[String, Int])]]] {

  def wordcount(chunk: Iterable[(String, String)]) = {
    val counts = Map[Int, Map[String, Int]]()

    for (i <- 0 until mappedPartitions) {
      counts(i) = Map[String, Int]()
    }

    for (pair <- chunk) {
      for (word <- pair._2.split("\\W+")) {
        val hash = word.hashCode().abs % mappedPartitions
        if (counts(hash).contains(word)) {
          counts(hash)(word) += 1
        } else {
          counts(hash)(word) = 1
        }
      }
    }

    counts.map((pair: (Int, Map[String, Int])) => {
      (pair._1, HashMap(pair._2.toSeq: _*))
    })
  }

  def reducer
      (pair1: (Int, HashMap[String, Int]),
       pair2: (Int, HashMap[String, Int])) = {
    val reduced =
      pair1._2.merged(pair2._2)({ case ((k, v1),(_, v2)) => (k, v1 + v2)})
    (pair1._1, reduced)
  }

  var mapped: AdjustableList[Int, HashMap[String, Int]] = _
  def run(implicit c: Context) = {
    mapped = list.hashChunkMap(wordcount)
    mapped.partitionedReduce(reducer)
  }
}

class WCChunkHashAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[String, Iterable[Mod[(Int, HashMap[String, Int])]]](_conf) {
  var data: FileData = null

  var input: Dataset[String, String] = null

  var adjust: WCChunkHashAdjust = null

  def generateNaive() {}

  def runNaive() {}

  override def loadInitial() {
    mutator.loadFile(conf.file)
    input = mutator.createList[String, String](
        conf.listConf.copy(file = conf.file))
          .asInstanceOf[Dataset[String, String]]

    adjust = new WCChunkHashAdjust(input.getAdjustableList(), 4)

    data = new FileData(
      mutator, input, conf.file, conf.updateFile, conf.runs)
  }

  def checkOutput(output: Iterable[Mod[(Int, HashMap[String, Int])]]) = {
    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("wc-output.txt"), "utf-8"))

    val sortedOutput = output.toBuffer.flatMap(
      (x: Mod[(Int, HashMap[String, Int])]) => {
        val v = mutator.read(x)

        if (v == null) {
          List()
        } else {
          v._2
        }
      }).sortWith(_._1 < _._1)

    for ((word, count) <- sortedOutput) {
      writer.write(word + " -> " + count + "\n")
    }
    writer.close()

    true
  }
}
