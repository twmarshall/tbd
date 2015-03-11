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

import scala.collection.immutable
import scala.collection.mutable.Map

import tdb._
import tdb.list._
import tdb.TDB._
import tdb.util._

class ColumnPageRankAdjust
    (links: ColumnListInput[Int], epsilon: Double, iters: Int)
  extends Adjustable[Unit] {

  def run(implicit c: Context) = {
    val aggregatorConf = AggregatorListConf(
      aggregator = (_: Double) + (_: Double),
      deaggregator = (_: Double) - (_: Double),
      initialValue = 0.0,
      threshold = (_: Double).abs > epsilon)

    for (i <- 0 until iters) {
      def mapper(key: Int, column1: Any, column2: Any, _c: Context) {
        val edges = column1.asInstanceOf[Array[Int]]
        val rank = column2.asInstanceOf[Double]
        val v = (rank / edges.size) * .85

        putAllIn(
          links,
          (i + 1) + "",
          (for (edge <- edges) yield (edge, v)) ++ Iterable(key -> .15))(_c)
      }

      links.getAdjustableList().projection2("edges", i + "", mapper)
    }
  }
}


class ColumnPageRankAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[Array[Int], Unit](_conf) {
  val columnConf = ColumnListConf(
    columns = immutable.Map(
      "key" -> (new StringColumn(), -1),
      "edges" -> (new StringColumn(), ""),
      "0" -> (AggregatedDoubleColumn(), 1.0),
      "1" -> (AggregatedDoubleColumn(), 0.0)))

  val input = mutator.createList[Int, Array[Int]](columnConf)
    .asInstanceOf[ColumnListInput[Int]]

  val data = new GraphData(input, conf.count, conf.mutations, conf.runs)
  //val data = new GraphFileData(input, "data.txt")
  //val data = new LiveJournalData(input)

  val adjust = new ColumnPageRankAdjust(
    input, conf.epsilon, conf.iters)

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

    for (i <- 0 until conf.iters) {
      println("links = " + links.map {
        case (k, v) => (k, v.mkString(","))
      })
      val joined = Map[Int, (Array[Int], Double)]()
      for ((url, rank) <- ranks) {
        joined(url) = (links(url), rank)
      }

      val contribs = joined.toSeq.flatMap { case (page, (links, rank)) =>
        val contrib = rank / links.size * .85
        links.map(url => (url, contrib)) ++ Iterable((page, .15))
      }

      val reducedContribs = Map[Int, Double]()
      for ((url, contrib) <- contribs) {
        reducedContribs(url) = contrib + reducedContribs.getOrElse(url, 0.0)
      }

      ranks = reducedContribs.map(pair => (pair._1, pair._2))
    }

    ranks
  }

  val epsilon = 0.1
  def checkOutput(output: Unit) = {
    val buf = input.getAdjustableList().toBuffer(mutator)
    val out = buf.map {
      case (k, v) => (k, v(conf.iters + "").asInstanceOf[Double])
    }

    val answer = naiveHelper(data.table)

    var check = out.size == answer.size
    var error = 0.0
    for ((node, rank) <- out) {
      error += (answer(node) - rank) / answer(node)
      if (!answer.contains(node)) {
        check = false
      }
    }

    val averageError = (error / answer.size).abs
    println("average error = " + averageError)
    println("output = " + out)
    println("answer = " + answer)

    check && averageError < epsilon
  }
}
