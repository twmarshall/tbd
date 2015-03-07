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

import scala.collection.GenIterable
import scala.collection.mutable.Map

import tdb._
import tdb.list._
import tdb.TDB._
import tdb.util._

object PageRankAlgorithm {
  val iters = 1
}

class PageRankAdjust(links: AdjustableList[Int, Array[Int]])
  extends Adjustable[AdjustableList[Int, Double]] {

  def run(implicit c: Context) = {
    val conf = ListConf.create(
      aggregate = true,
      aggregator = (_: Double) + (_: Double),
      deaggregator = (_: Double) - (_: Double),
      initialValue = 0.0)

    def innerPageRank(i: Int): ListInput[Int, Double] = {
      if (i == 0) {
        val ranks = createList[Int, Double](conf)

        /*links.foreachChunk {
         case (chunk, c) =>
         putAll(r, chunk.map(pair => (pair._1, 1.0)))(c)
         }*/

        links.foreach {
          case (pair, c) => put(ranks, pair._1, 1.0)(c)
        }
        ranks
      } else {
        val ranks = innerPageRank(i - 1)
        val newRanks = createList[Int, Double](conf)
        /*def chunkMapper(nodes: Iterable[(Int, Array[Int])], c: Context) {
         for ((node, edges) <- nodes) {
         get(r, node) {
         case rank =>
         put(newRanks, node, 0.15)(c)
         val v = (rank / edges.size) * .85
         for (edge <- edges) {
         put(newRanks, edge, v)(c)
         }
         }(c)
         }
         }
         links.foreach(chunkMapper)*/

        def mapper(pair: (Int, Array[Int]), c: Context) {
          get(ranks, pair._1) {
            case rank =>
              put(newRanks, pair._1, 0.15)(c)
              val v = (rank / pair._2.size) * .85
              for (edge <- pair._2) {
                put(newRanks, edge, v)(c)
              }
          }(c)
        }

        links.foreach(mapper)

        newRanks
      }
    }

    innerPageRank(PageRankAlgorithm.iters).getAdjustableList()
  }
}


class PageRankAlgorithm(_conf: AlgorithmConf)
    extends Algorithm[Array[Int], AdjustableList[Int, Double]](_conf) {
  val input = mutator.createList[Int, Array[Int]](conf.listConf)

  val data = new GraphData(input, conf.count, conf.mutations, conf.runs)
  //val data = new GraphFileData(input, "data.txt")

  val adjust = new PageRankAdjust(input.getAdjustableList())

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

    for (i <- 1 to PageRankAlgorithm.iters) {
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

  val epsilon = 0.000001
  def checkOutput(output: AdjustableList[Int, Double]) = {
    val out = output.toBuffer(mutator)
    val answer = naiveHelper(data.table)

    var check = out.size == answer.size
    for ((node, rank) <- out) {
      if (!answer.contains(node) || (answer(node) - rank).abs > epsilon) {
        check = false
      }
    }

    //println("output = " + out)
    //println("answer = " + answer)

    check
  }
}
