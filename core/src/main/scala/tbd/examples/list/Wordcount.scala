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
package tbd.examples

import akka.util.Timeout
import java.io.{BufferedWriter, FileWriter}
import org.rogach.scallop._
import scala.collection.immutable.HashMap
import scala.collection.mutable.Map
import scala.concurrent.duration._

import tbd.{Adjustable, Constants, Context, Mod, Mutator}
import tbd.list.{AdjustableList, ListConf}
import tbd.master.MasterConnector
import tbd.TBD._

class WordcountAdjust(list: AdjustableList[String, String])
  extends Adjustable[Mod[(String, HashMap[String, Int])]] {

  def run(implicit c: Context) = {
    val counts = list.map(Wordcount.mapper)
    counts.reduce(Wordcount.reducer)
    /*mod {
      write(("", HashMap[String, Int]()))
    }*/
  }
}

class WordcountHashAdjust(list: AdjustableList[String, String], mappedPartitions: Int)
    extends Adjustable[Iterable[Mod[(String, HashMap[String, Int])]]] {

  def wordcount(pair: (String, String)): Map[String, HashMap[String, Int]] = {
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
      (pair._1 + "", HashMap(pair._2.toSeq: _*))
    })
  }

  def run(implicit c: Context) = {
    val mapped = list.hashPartitionedFlatMap(wordcount, mappedPartitions)
    mapped.partitionedReduce(Wordcount.reducer)
    /*Array(mod {
      write(("", HashMap[String, Int]()))
    })*/
  }
}

class ChunkWordcountAdjust(list: AdjustableList[String, String])
  extends Adjustable[Mod[(String, HashMap[String, Int])]] {

  def run(implicit c: Context): Mod[(String, HashMap[String, Int])] = {
    val counts = list.chunkMap(Wordcount.chunkMapper)
    counts.reduce(Wordcount.reducer)
  }
}

object Wordcount {
  def wordcount(s: String): HashMap[String, Int] = {
    HashMap(mutableWordcount(s).toSeq: _*)
  }

  def mutableWordcount
      (s: String,
       counts: Map[String, Int] = Map[String, Int]()) : Map[String, Int] = {
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

  def mapper(pair: (String, String)) = {
    (pair._1, wordcount(pair._2))
  }

  def reducer
      (pair1: (String, HashMap[String, Int]),
       pair2: (String, HashMap[String, Int])) = {
    (pair1._1, reduce(pair1._2, pair2._2))
  }

  def chunkMapper(chunk: Vector[(String, String)]) = {
    var counts = Map[String, Int]()

    for (page <- chunk) {
      counts = mutableWordcount(page._2, counts)
    }

    ("", HashMap(counts.toSeq: _*))
  }

  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TBD 0.1 (c) 2014 Carnegie Mellon University")
      banner("Usage: master.sh [options]")
      val algorithm = opt[String]("algorithm", 'a', default = Some("wc"))
      val chunkSize = opt[Int]("chunkSize", 'c', default = Some(1))
      val file = opt[String]("file", 'f', default = Some("wiki.xml"))
      val partitions = opt[Int]("partitions", 'p', default = Some(1))
      val timeout = opt[Int]("timeout", 't', default = Some(100))

      val master = trailArg[String](required = true)
    }

    Constants.DURATION = Conf.timeout.get.get.seconds
    Constants.TIMEOUT = Timeout(Constants.DURATION)

    val algorithm = Conf.algorithm.get.get

    val mutator = new Mutator(MasterConnector(Conf.master.get.get))

    val listConf = new ListConf(
      file = Conf.file.get.get,
      partitions = Conf.partitions.get.get,
      chunkSize = Conf.chunkSize.get.get,
      double = true)

    val beforeLoad = System.currentTimeMillis()
    var input = mutator.createList[String, String](listConf)
    println("load time = " + (System.currentTimeMillis() - beforeLoad))

    if (algorithm == "wc") {
      val beforeInitial = System.currentTimeMillis()
      val output = mutator.run(new WordcountAdjust(input.getAdjustableList()))
      println("initial run time = " + (System.currentTimeMillis - beforeInitial))

      //val file = new BufferedWriter(new FileWriter("wordcount.out"))
      //file.write(mutator.read(output)._2.toString)
    } else {
      val beforeInitial = System.currentTimeMillis()
      val output = mutator.run(new WordcountHashAdjust(input.getAdjustableList(), listConf.partitions))
      println("initial run time = " + (System.currentTimeMillis - beforeInitial))
    }
      /*} else {
        mutator.run(new ChunkWordcountAdjust(input.getAdjustableList()))
      }*/

    mutator.shutdown()
  }
}
