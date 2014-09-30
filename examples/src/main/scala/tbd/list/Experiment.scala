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
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.duration._

import tbd.{Constants, Mutator}
import tbd.list.ListConf

object Experiment {
  val usage ="""Usage: run.sh [OPTION]...

Options:
  -a, --algorithms s,s,...   Algorithms to run, where s could be: filter,
                               flatMap, join, map, msort, pgrank, qsort, rbk,
                               sjoin, split, or wc.
  -c, --check                Turns output checking on, for debugging.
  -f, --file                 File to read the workload from. If none is
                               specified, the data will be randomly generated.
                               This option overrides --counts and --runs.
  -h, --help                 Display this message.
  -m, --mutations s,s,...    Mutations to perform on the input data. Must be
                               one of 'update', 'insert', or 'remove'.
  -n, --counts n,n,...       Number of chunks to load initially.
  -o, --output chart,line,x  How to format the printed results - each of
                               'chart', 'line', and 'x' must be one of
                               'algorithms', 'chunkSizes', 'counts',
                               'partitons', or 'runs', with one required
                               to be 'runs'.
  -p, --partitions n,n,...   Number of partitions for the input data.
  -s, --chunkSizes n,n,...   Size of each chunk in the list, in KB.
  -r, --runs f,f,...         What test runs to execute. 'naive' and 'initial'
                               are included automatically, so this is a list
                               of update sizes (f >= 1) or update percentages
                               (0 < f < 1).
  -v, --verbosity            Adjusts the amount of output, with 0 indicating no
                               output. (default: '1')
  --repeat n                 Number of times to repeat each experiment.
  --load                     If specified, loading times will be included in
                               formatted output.
  --store type               The type of datastore to use, either 'memory' or
                               'berkeleydb' (default: 'memory').
  --cacheSizes n,n,...       The number of mods to store in the in-memory cache
                               when using the 'berkeleydb' store.
  """

  var repeat = 3

  var verbosity = 1

  var check = false

  var displayLoad = false

  var file = ""

  val confs = Map(("algorithms" -> Array("map")),
                  ("cacheSizes" -> Array("100000")),
                  ("counts" -> Array("1000")),
                  ("chunkSizes" -> Array("2")),
                  ("mutations" -> Array("insert", "update", "remove")),
                  ("partitions" -> Array("8")),
                  ("runs" -> Array("naive", "initial", ".01", ".05", ".1")),
                  ("output" -> Array("algorithms", "runs", "counts")),
		  ("store" -> Array("memory")))

  val allResults = Map[Map[String, _], Map[String, Double]]()

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
  def printCharts(charts: String, lines: String, x: String) {
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
    Constants.DURATION = 1000.seconds
    Constants.TIMEOUT = Timeout(1000.seconds)

    var i = 0
    while (i < args.size) {
      args(i) match {
        case "--algorithms" | "-a" =>
          confs("algorithms") = args(i + 1).split(",")
	  i += 1
	case "--check" | "-c" =>
	  check = true
        case "--counts" | "-n" =>
          confs("counts") = args(i + 1).split(",")
	  i += 1
	case "--file" | "-f" =>
	  file = args(i + 1)
	  i += 1
        case "--help" | "-h" =>
          println(usage)
          sys.exit()
        case "--mutations" | "-m" =>
          confs("mutations") = args(i + 1).split(",")
	  i += 1
        case "--partitions" | "-p" =>
          confs("partitions") = args(i + 1).split(",")
	  i += 1
        case "--runs" | "-r" =>
          confs("runs") = "naive" +: "initial" +: args(i + 1).split(",")
	  i += 1
        case "--output" | "-o" =>
          confs("output") = args(i + 1).split(",")
	  i += 1
          assert(confs("output").size == 3)
	case "--chunkSizes" | "-s" =>
	  confs("chunkSizes") = args(i + 1).split(",")
	  i += 1
	case "--verbosity" | "-v" =>
	  verbosity = args(i + 1).toInt
	  i += 1
        case "--repeat" =>
          repeat = args(i + 1).toInt
	  i += 1
	case "--load" =>
	  displayLoad = true
	case "--store" =>
	  confs("store") = Array(args(i + 1))
	  i += 1
	case "--cacheSizes" =>
	  confs("cacheSizes") = args(i + 1).split(",")
	  i += 1
        case _ =>
          println("Unknown option " + args(i * 2) + "\n" + usage)
          sys.exit()
      }

      i += 1
    }

    if (!confs("output").contains("runs")) {
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
    }

    run(confs)
  }
 
  def run(confs: Map[String, Array[String]]) {
    for (i <- 0 to repeat) {
      if (verbosity > 0) {
	if (i == 0) {
          println("warmup")
	} else if (i == 1) {
          println("done warming up")
	}
      }

      for (algorithm <- confs("algorithms")) {
	for (cacheSize <- confs("cacheSizes")) {
	  for (chunkSize <- confs("chunkSizes")) {
            for (count <- confs("counts")) {
              for (partitions <- confs("partitions")) {
		val conf = Map(("algorithms" -> algorithm),
			       ("cacheSizes" -> cacheSize),
			       ("chunkSizes" -> chunkSize),
			       ("counts" -> count),
			       ("mutations" -> confs("mutations")),
			       ("partitions" -> partitions),
			       ("runs" -> confs("runs")),
			       ("repeat" -> i),
			       ("store" -> confs("store")(0)))

		val listConf = new ListConf("", partitions.toInt,
					    chunkSize.toInt, _ => 1)

		val alg = algorithm match {
		  case "filter" => new FilterAlgorithm(conf, listConf)

		  case "flatMap" => new FlatMapAlgorithm(conf, listConf)

		  case "join" =>
		    if (listConf.chunkSize > 1)
		      new ChunkJoinAlgorithm(conf, listConf)
		    else
		      new JoinAlgorithm(conf, listConf)

		  case "map" =>
		    new MapAlgorithm(conf, listConf)

		  case "msort" =>
		    new MergeSortAlgorithm(conf, listConf)

		  case "pgrank" => new PageRankAlgorithm(conf, listConf)

		  case "qsort" =>
		    new QuickSortAlgorithm(conf, listConf)

		  case "rbk" => new ReduceByKeyAlgorithm(conf, listConf)

		  case "sjoin" =>
		    new SortJoinAlgorithm(conf, listConf)

		  case "split" =>
		    new SplitAlgorithm(conf, listConf)

		  case "wc" =>
		    if (listConf.chunkSize > 1)
		      new ChunkWCAlgorithm(conf, listConf)
		    else
		      new WCAlgorithm(conf, listConf)

                  case "wsort" =>
                    new WikiSortAlgorithm(conf, listConf)
		}

		val results = alg.run()

		if (verbosity > 0) {
		  println(results)
		}

		if (i != 0) {
		  Experiment.allResults += (conf -> results)
		}
	      }
            }
          }
        }
      }
    }

    if (verbosity > 0) {
      printCharts(confs("output")(0), confs("output")(1), confs("output")(2))
    }
  }
}
