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

import akka.util.Timeout
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.duration._

import tdb.{Constants, Mutator}
import tdb.list.ListConf

object Experiment {

  // If true, the experiment will run faster but the timing may not be
  // completely accurate.
  var fast = false

  var conf: ExperimentConf = null

  var verbosity = 1

  var check = false

  var port = 2252

  val allResults = Map[AlgorithmConf, Map[String, Double]]()

  val confs = Map[String, List[String]]()

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
    for (chart <- confs(charts)) {
      print(chart + "\t")
      for (line <- confs(lines)) {
        print(line + "\t")
      }
      print("\n")

      for (xValue <- confs(x)) {
        print(xValue)

        for (line <- confs(lines)) {
          var total = 0.0
          var loadTotal = 0.0
          var gcTotal = 0.0
          var repeat = 0

          for ((conf, results) <- allResults) {
            if (charts == "runs") {
              if (conf(lines) == line &&
                  conf(x) == xValue) {
                total += results(chart)
                loadTotal += results(chart + "-load")
                gcTotal += results(chart + "-gc")
                repeat += 1
              }
            } else if (lines == "runs") {
              if ((x == "breakdown" || conf(x) == xValue) &&
                  conf(charts) == chart) {
                total += results(line)
                loadTotal += results(line + "-load")
                gcTotal += results(line + "-gc")
                repeat += 1
              }
            } else if (x == "runs") {
              if (conf(charts) == chart &&
                  conf(lines) == line) {
                total += results(xValue)
                loadTotal += results(xValue + "-load")
                gcTotal += results(xValue + "-gc")
                repeat += 1
              }
            } else {
              println("Warning: print must vary 'runs'")
            }
          }

          val load = round(loadTotal / repeat)
          val gc = round(gcTotal / repeat)
          val rest = round(total / repeat) - gc
          val totalAverage = round(load + gc + rest)

          if (chart == "load" || xValue == "load" || line == "load") {
            print("\t" + load)
          } else if (chart == "gc" || xValue == "gc" || line == "gc") {
            print("\t" + gc)
          } else if (chart == "rest" || xValue == "rest" || line == "rest") {
            print("\t" + rest)
          } else {
            print("\t" + totalAverage)
          }
        }
        print("\n")
      }
    }
  }

  def main(args: Array[String]) {
    conf = new ExperimentConf(args)

    verbosity = conf.verbosity()
    check = conf.check()

    Constants.DURATION = conf.timeout().seconds
    Constants.TIMEOUT = Timeout(Constants.DURATION)

    run(conf)
  }

  def run(conf: ExperimentConf) {
    fast = conf.fast()
    confs("algorithms") = conf.algorithms()
    confs("counts") = conf.counts()
    confs("files") = conf.files()
    confs("runs") = conf.runs()
    confs("partitions") = conf.partitions()
    confs("mutations") = conf.mutations()
    confs("output") = conf.output()
    confs("chunkSizes") = conf.chunkSizes()
    confs("cacheSizes") = conf.cacheSizes()
    confs("epsilons") = conf.epsilons().map(_.toString)
    confs("iters") = conf.iters().map(_.toString)
    confs("breakdown") = List("load", "gc", "rest", "total")

    if (conf.verbosity() > 0) {
      if (!conf.output().contains("runs")) {
        println("WARNING: 'output' must contain 'runs'")
      }

      if (conf.output().size != 3) {
        println("Must specify three parameters to output")
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
      }
    }

    for (i <- 0 to conf.repeat()) {
      if (verbosity > 0 && conf.repeat() > 0) {
        if (i == 0) {
          println("warmup")
        } else if (i == 1) {
          println("done warming up")
        }
      }

      for (algorithm <- conf.algorithms();
           cacheSize <- conf.cacheSizes();
           chunkSize <- conf.chunkSizes();
           count <- conf.counts();
           file <- conf.files();
           partitions <- conf.partitions();
           iters <- conf.iters();
           epsilon <- conf.epsilons()) {
        val listConf = ListConf(
          file = file,
          partitions = partitions.toInt,
          chunkSize = chunkSize.toInt)

        val algConf = AlgorithmConf(
          algorithm = algorithm,
          cacheSize = cacheSize.toInt,
          count = count.toInt,
          envHomePath = conf.envHomePath(),
          file = file,
          logLevel = conf.log(),
          master = conf.master(),
          mutations = conf.mutations(),
          naive = conf.naive(),
          runs = conf.runs(),
          repeat = i,
          storeType = conf.store(),
          updateFile = conf.updateFile(),
          updateRepeat = conf.updateRepeat(),
          listConf = listConf,
          iters = iters,
          epsilon = epsilon)

        val alg = algorithm match {
          case "map" =>
            new MapAlgorithm(algConf)
          case "pgrank" => new PageRankAlgorithm(algConf)
          case "cpgr" => new ColumnPageRankAlgorithm(algConf)
          case "rpgr" => new ReversePageRankAlgorithm(algConf)
          case "wc" =>
              new WCAlgorithm(algConf)
          case "wch" =>
            new WCHashAlgorithm(algConf)
          case "cw" =>
            new CWChunkHashAlgorithm(algConf)
          case "rcw" =>
            new RandomCWAlgorithm(algConf)
        }

        val results = alg.run()

        if (verbosity > 0) {
          println(results)
        }

        if (i != 0 || conf.repeat() == 0) {
          Experiment.allResults += (algConf -> results)
        }
      }
    }

    if (verbosity > 0) {
      printCharts(conf.output()(0), conf.output()(1), conf.output()(2), conf)
    }
  }
}
