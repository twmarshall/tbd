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
package thomasdb.examples.test

import org.scalatest._

import thomasdb.examples.list._

class AlgorithmTests extends FlatSpec with Matchers {
  Experiment.verbosity = 0
  Experiment.check = true
  Experiment.port = 2553

  val defaults = Array("--verbosity", "0")

  val intensity = 10

  "FilterTest" should "run filter successfully." in {
    val conf = new ExperimentConf(
      Array("--algorithms", "filter",
            "--chunkSizes", "1",
            "--counts", intensity.toString,
            "--partitions", "1", "4",
            "--repeat", "0") ++ defaults)

    Experiment.run(conf)
  }

  "FlatMapTest" should "run flatMap successfully" in {
    val conf = new ExperimentConf(
      Array("--algorithms", "flatMap",
            "--chunkSizes", "1", "4",
            "--counts", intensity.toString,
            "--partitions", "1", "4") ++ defaults)

    Experiment.run(conf)
  }

  "JoinTest" should "run join successfully." in {
    val nestedLoopConf = new ExperimentConf(
      Array("--algorithms", "join",
            "--chunkSizes", "1", "4",
            "--counts", intensity.toString,
            "--partitions", "1") ++ defaults)

    Experiment.run(nestedLoopConf)


    val sortConf = new ExperimentConf(
      Array("--algorithms", "sjoin",
            "--chunkSizes", "1",
            "--counts", intensity.toString,
            "--partitions", "1") ++ defaults)

    Experiment.run(sortConf)
  }

  "MapTest" should "run map successfully." in {
    val conf = new ExperimentConf(
      Array("--algorithms", "map",
            "--chunkSizes", "1", "4",
            "--counts", intensity.toString,
            "--files", "wiki.xml",
            "--partitions", "1", "4") ++ defaults)

    Experiment.run(conf)
  }

  /*"PageRankTest" should "run page rank successfully." in {
    val conf = Map(
      "algorithms" -> Array("pgrank"),
      "chunkSizes" -> Array("1"),
      "counts" -> Array(intensity.toString),
      "partitions" -> Array("1"))

    Experiment.run(Experiment.confs ++ conf)
  }*/

  "ReduceByKeyTest" should "run reduceByKey successfully" in {
    val conf = new ExperimentConf(
      Array("--algorithms", "rbk",
            "--chunkSizes","1", "4",
            "--counts", intensity.toString,
            "--partitions", "1") ++ defaults)

    Experiment.run(conf)
  }

  "SortTest" should "run sort successfully." in {
    val quickConf = new ExperimentConf(
      Array("--algorithms", "qsort",
            "--chunkSizes", "1",
            "--counts", intensity.toString,
            "--partitions", "1", "4") ++ defaults)

    Experiment.run(quickConf)

    val mergeConf = new ExperimentConf(
      Array("--algorithms", "msort",
            "--chunkSizes", "1", "2",
            "--counts", intensity.toString,
            "--partitions", "1", "4") ++ defaults)

    Experiment.run(mergeConf)
  }

  "SplitTest" should "run split successfully." in {
    val conf = new ExperimentConf(
      Array("--algorithms", "split",
            "--chunkSizes", "1",
            "--counts", intensity.toString,
            "--partitions", "1") ++ defaults)

    Experiment.run(conf)
  }

  /*"WordcountTest" should "run wordcount successfully." in {
    val conf = Map(
      "algorithms" -> Array("wc"),
      "chunkSizes" -> Array("1", "4"),
      "counts" -> Array(intensity.toString),
      "partitions" -> Array("1", "4"))

    Experiment.run(Experiment.confs ++ conf)
  }*/
}
