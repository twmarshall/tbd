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

import scala.collection.GenIterable
import scala.collection.mutable.Map

import tdb._
import tdb.list._
import tdb.TDB._
import tdb.util._

class PageRankAdjust
    (links: AdjustableList[Int, Array[Int]], epsilon: Double, iters: Int)
  extends Adjustable[AdjustableList[Int, Double]] {

  def run(implicit c: Context) = {
    val aggregatorConf = AggregatorListConf(
      valueType = AggregatedDoubleColumn(epsilon))

    def innerPageRank(i: Int): ListInput[Int, Double] = {
      if (i == 1) {
        val newRanks = createList[Int, Double](aggregatorConf)

        def mapper(pair: (Int, Array[Int]), c: Context) {
          val rank = 1.0
          put(newRanks, pair._1, 0.15)(c)
          val v = (rank / pair._2.size) * .85
          putAll(newRanks, for (edge <- pair._2) yield (edge, v))(c)
        }

        links.foreach(mapper)

        newRanks
      } else {
        val ranks = innerPageRank(i - 1)
        val newRanks = createList[Int, Double](aggregatorConf)

        def mapper(pair: (Int, Array[Int]), c: Context) {
          put(newRanks, pair._1, 0.15)(c)
          get(ranks, pair._1) {
            case rank =>
              val v = (rank / pair._2.size) * .85
              putAll(newRanks, for (edge <- pair._2) yield (edge, v))(c)
          }(c)
        }

        links.foreach(mapper)

        newRanks
      }
    }

    innerPageRank(iters).getAdjustableList()
  }
}

class PageRankAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[AdjustableList[Int, Double]](_conf) {
  val input = mutator.createList[Int, Array[Int]](conf.listConf.clone(file = ""))

  //val data = new GraphData(input, conf.count, conf.mutations, conf.runs)
  val data = new GraphFileData(input, conf.file, conf.runs, conf.updateRepeat)
  //val data = new LiveJournalData(input)

  val adjust = new PageRankAdjust(
    input.getAdjustableList(), conf.epsilon, conf.iters)

  var naiveTable: Map[Int, Array[Int]] = _
  def generateNaive() {
    data.generate()
    naiveTable = data.table
  }

  def runNaive() {
    naiveHelper(naiveTable)
  }

  private def naiveHelper(links: Map[Int, Array[Int]]) = {
    var ranks = links.map(pair => (pair._1, 1.0))

    for (i <- 1 to conf.iters) {
      val joined = Map[Int, (Array[Int], Double)]()
      for ((url, rank) <- ranks) {
        joined(url) = (links(url), rank)
      }

      val contribs = joined.toSeq.flatMap { case (page, (links, rank)) =>
        val contrib = rank / links.size * .85
        links.map(url => (url, contrib)) ++ Iterable((page, .15))
      }

      ranks = Map[Int, Double]()
      for ((url, contrib) <- contribs) {
        ranks(url) = contrib + ranks.getOrElse(url, 0.0)
      }
    }

    ranks
  }

  def loadInitial() {
    data.load()
  }

  def hasUpdates() = data.hasUpdates()

  def loadUpdate() = data.update()

  val epsilon = 0.1
  def checkOutput(output: AdjustableList[Int, Double]) = {
    val out = output.toBuffer(mutator)
    val answer = naiveHelper(data.table)

    var check = out.size == answer.size
    var totalError = 0.0
    var maxError = 0.0
    for ((node, rank) <- out) {
      if (!answer.contains(node)) {
        check = false
      } else {
        val thisError = (answer(node) - rank) / answer(node)

        if (thisError > maxError) {
          maxError = thisError
        }

        totalError += thisError
      }
    }

    val averageError = (totalError / answer.size).abs * 100
    println("average error = " + averageError + "%")
    println("max error = " + (maxError * 100) + "%")
    //println("output = " + out.sortWith(_._1 < _._1))
    //println("answer = " + answer.toBuffer.sortWith(_._1 < _._1))

    check && averageError < epsilon
  }
}
