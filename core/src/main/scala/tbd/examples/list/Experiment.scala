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

import akka.util.Timeout
import org.rogach.scallop._
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.duration._

import tbd.{Constants, Mutator}
import tbd.list.ListConf

object Experiment {

  var verbosity = 1

  var check = false

  var displayLoad = false

  var file = ""

  var master = ""

  var port = 2252

  val allResults = Map[AlgorithmConf, Map[String, Double]]()

  def round(value: Double): Double = {
    BigDecimal(value).setScale(2, BigDecimal.RoundingMode.HALF_UP).toDouble
  }

  /**
   * Prints out results formatted for copy-paste into spreadsheets. The output
   * will be divided into blocks differing along the 'charts' dimension, with
   * each block varying 'lines' along one dimension and 'x' along another.
   *
   * Either 'charts', 'lines', or 'x' must be 'runs', since the results
   * don't make any sense unless you vary initial vs. propagation. They must
   * also be three distinct values.
   */
  def printCharts
      (charts: String, lines: String, x: String, conf: ExperimentConf) {
    val confs = Map[String, List[String]]()
    confs("algorithms") = conf.algorithms.get.get
    confs("runs") = conf.runs.get.get
    confs("counts") = conf.counts.get.get

    for (chart <- confs(charts)) {
      print(chart + "\t")
      for (line <- confs(lines)) {
        print(line + "\t")
        print("no gc\t")
        if (displayLoad) {
          print("load\t")
        }
      }
      print("\n")

      for (xValue <- confs(x)) {
        print(xValue)

        for (line <- confs(lines)) {
          var total = 0.0
          var loadTotal = 0.0
          var noGCTotal = 0.0
          var repeat = 0

          for ((conf, results) <- allResults) {
            if (charts == "runs") {
              if (conf(lines) == line &&
                  conf(x) == xValue) {
                total += results(chart)
                loadTotal += results(chart + "-load")
                noGCTotal += results(chart + "-nogc")
                repeat += 1
              }
            } else if (lines == "runs") {
              if (conf(x) == xValue &&
                  conf(charts) == chart) {
                total += results(line)
                loadTotal += results(line + "-load")
                noGCTotal += results(line + "-nogc")
                repeat += 1
              }
            } else if (x == "runs") {
              if (conf(charts) == chart &&
                  conf(lines) == line) {
                total += results(xValue)
                loadTotal += results(xValue + "-load")
                noGCTotal += results(xValue + "-nogc")
                repeat += 1
              }
            } else {
              println("Warning: print must vary 'runs'")
            }
          }

          print("\t" + round(total / repeat))
          print("\t" + round(noGCTotal / repeat))
          if (displayLoad) {
            print("\t" + round(loadTotal / repeat))
          }
        }
        print("\n")
      }
    }
  }

  def main(args: Array[String]) {
    val conf = new ExperimentConf(args)

    verbosity = conf.verbosity.get.get
    check = conf.check.get.get
    displayLoad = conf.load.get.get
    file = conf.file.get.get
    master = conf.master.get.get

    Constants.DURATION = 1000.seconds
    Constants.TIMEOUT = Timeout(1000.seconds)

    /*if (!confs("output").contains("runs")) {
      println("WARNING: 'output' must contain 'runs'")
    }

    println("Options:")
    for ((key, value) <- confs) {
      print(key)

      if (key.size < 7) {
        print("\t\t")
      } else {
        print("\t")
      }

      println(value.mkString("(", ", ", ")"))

      if (key != "output" && key != "mutations" &&
          value.size > 1 && !confs("output").contains(key)) {
        println("WARNING: " + key + " is being varied but isn't listed in " +
                "'output', so the final results may not make sense.")
      }
    }*/

    run(conf)
  }

  def run(conf: ExperimentConf) {
    for (i <- 0 to conf.repeat.get.get) {
      if (verbosity > 0) {
        if (i == 0) {
          println("warmup")
        } else if (i == 1) {
          println("done warming up")
        }
      }

      for (algorithm <- conf.algorithms()) {
        for (cacheSize <- conf.cacheSizes()) {
          for (chunkSize <- conf.chunkSizes()) {
            for (count <- conf.counts()) {
              for (partitions <- conf.partitions()) {
                val listConf = new ListConf(
                  partitions = partitions.toInt,
                  chunkSize = chunkSize.toInt)

                val algConf = AlgorithmConf(
                  algorithm = algorithm,
                  cacheSize = cacheSize.toInt,
                  count = count.toInt,
                  mutations = conf.mutations(),
                  runs = conf.runs(),
                  repeat = i,
                  store = conf.store(),
                  listConf = listConf)

                val alg = algorithm match {
                  case "filter" => new FilterAlgorithm(algConf)

                  case "flatMap" => new FlatMapAlgorithm(algConf)

                  case "join" =>
                    if (listConf.chunkSize > 1)
                      new ChunkJoinAlgorithm(algConf)
                    else
                      new JoinAlgorithm(algConf)

                  case "map" =>
                    new MapAlgorithm(algConf)

                  case "msort" =>
                    new MergeSortAlgorithm(algConf)

                  case "pgrank" => new PageRankAlgorithm(algConf)

                  case "qsort" =>
                    new QuickSortAlgorithm(algConf)

                  case "rbk" => new ReduceByKeyAlgorithm(algConf)

                  case "sjoin" =>
                    new SortJoinAlgorithm(algConf)

                  case "split" =>
                    new SplitAlgorithm(algConf)

                  case "wc" =>
                    if (listConf.chunkSize > 1)
                      new ChunkWCAlgorithm(algConf)
                    else
                      new WCAlgorithm(algConf)
                  case "wch" =>
                    new WCHashAlgorithm(algConf)
                }

                val results = alg.run()

                if (verbosity > 0) {
                  println(results)
                }

                if (i != 0) {
                  Experiment.allResults += (algConf -> results)
                }
              }
            }
          }
        }
      }
    }

    if (verbosity > 0) {
      printCharts(conf.output()(0), conf.output()(1), conf.output()(2), conf)
    }
  }
}

class ExperimentConf(_args: Array[String]) extends ScallopConf(_args) {
  version("TBD 0.1 (c) 2014 Carnegie Mellon University")
  banner("Usage: experiment.sh [options]")
  val algorithms = opt[List[String]]("algorithms", 'a',
    default = Some(List("wc")), descr = "Algorithms to run, where s " +
    "could be: filter, flatMap, join, map, msort, pgrank, qsort, rbk, " +
    "sjoin, split, or wc.")
  val check = opt[Boolean]("check", 'c', default = Some(false),
    descr = "Turns output checking on, for debugging.")
  val counts = opt[List[String]]("counts", 'n',
    default = Some(List("1000")),
    descr = "Number of elements to load initially.")
  val file = opt[String]("file", 'f', default = Some("wiki.xml"),
    descr = "File to read the workload from. If none is specified, the " +
    "data will be randomly generated. This option overrides --counts and " +
    "--runs.")
  val mutations = opt[List[String]]("mutations",
    default = Some(List("insert", "update", "remove")),
    descr = "Mutations to perform on the input data. Must be one of " +
    "'update', 'insert', or 'remove'.")
  val partitions = opt[List[String]]("partitions", 'p',
    default = Some(List("8")),
    descr = "Number of partitions to divide the input into.")
  val runs = opt[List[String]]("runs", 'r',
    default = Some(List("1", "10")),
    descr = "What test runs to execute. 'naive' and 'initial' are " +
    "included automatically, so this is a list of update sizes (f >= 1) " +
    "or update percentages (0 < f < 1).")
  val output = opt[List[String]]("output", 'o',
    default = Some(List("algorithms", "runs", "counts")),
    descr = "How to format the printed results - each of 'chart', " +
    "'line', and 'x' must be one of 'algorithms', 'chunkSizes', " +
    "'counts', 'partitons', or 'runs', with one required to be 'runs'.")
  val chunkSizes = opt[List[String]]("chunkSizes", 's',
    default = Some(List("1")),
    descr = "Size of each chunk, by number of elements")
  val verbosity = opt[Int]("verbosity", 'v', default = Some(1),
    descr = "Adjusts the amount of output, with 0 indicating no output.")
  val repeat = opt[Int]("repeat", 'q', default = Some(3),
    descr = "The number of times to repeat the test.")
  val load = opt[Boolean]("load", 'l', default = Some(false),
    descr = "If true, loading times will be included in output.")
  val store = opt[String]("store", 'w', default = Some("memory"),
    descr = "The data store type to use - memory or berkeleydb.")
  val cacheSizes = opt[List[String]]("cacheSizes", 'h',
    default = Some(List("10000")),
    descr = "The size of the cache.")
  val master = opt[String]("master", default = None,
    descr = "The master Akka url to connect to. If unspecified, we'll " +
    "launch a master.")
}

case class AlgorithmConf(
  algorithm: String,
  cacheSize: Int,
  count: Int,
  mutations: List[String],
  runs: List[String],
  repeat: Int,
  store: String,
  listConf: ListConf) {

  def apply(param: String): String =
    param match {
      case "algorithms" => algorithm
      case "counts" => count.toString
    }
}
