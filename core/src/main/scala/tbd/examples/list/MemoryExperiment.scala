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
package tbd.examples.list

import scala.collection.mutable.Map
import scala.collection.parallel.{ForkJoinTaskSupport, ParIterable}
import scala.concurrent.forkjoin.ForkJoinPool
import scala.collection.immutable.HashMap
import akka.util.Timeout
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.duration._

import tbd._
import tbd.datastore.StringData
import tbd.list._

class MemoryExperiment(input: ListInput[Int, String])
    extends Adjustable[Mod[(Int, HashMap[String, Int])]] {
  def mapper(pair: (Int, String)) = {
    (pair._1, WCAlgorithm.wordcount(pair._2))
  }

  def reducer(
      pair1: (Int, HashMap[String, Int]),
      pair2: (Int, HashMap[String, Int])) = {
    (pair1._1, WCAlgorithm.reduce(pair1._2, pair2._2))
   }

  def chunkMapper(chunk: Vector[(Int, String)]) = {
    var counts = Map[String, Int]()

    for (page <- chunk) {
      counts = WCAlgorithm.mutableWordcount(page._2, counts)
    }

    (0, HashMap(counts.toSeq: _*))
  }

  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    val counts = list.chunkMap(chunkMapper)
    counts.reduce(reducer)
  }
}

object MemoryExperiment {
  def generate(count: Int) = {
  var vec = Vector[String]().par
    while (vec.size < count) {
      val elems = scala.xml.XML.loadFile("wiki.xml")

      (elems \\ "elem").map(elem => {
        (elem \\ "value").map(value => {
        if (vec.size < count) {
	  vec :+= value.text
	  }
        })
      })
    }
    vec
  }

  def main(args: Array[String]) {
    Constants.DURATION = 1000.seconds
    Constants.TIMEOUT = Timeout(1000.seconds)
    for (max <- List(1000)) {
      println(max)
      //val input = generate(max)

      //val output = input.aggregate(Map[String, Int]())((x, line) =>
      //  WCAlgorithm.countReduce(line, x), WCAlgorithm.mutableReduce)

      val mutator = new Mutator()
      val list = mutator.createList[Int, String](new ListConf(partitions = 4, chunkSize = 100))
      val input = new StringData(list, max, Array("insert", "remove", "update"), false)
      input.generate()
      input.load()
      input.clearValues()
      val output = mutator.run(new MemoryExperiment(list))

      val rand = new scala.util.Random()
      var i = 0
      while (i < 500) {
	println(i)
        input.update(1)

        mutator.propagate()
        i += 1
      }
      mutator.shutdown()

      println("done")
    }
  }
}
