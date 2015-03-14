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
package tdb.examples.test

import org.scalatest._

import tdb.examples._

class AlgorithmTests extends FlatSpec with Matchers {
  Experiment.verbosity = 0
  Experiment.check = true
  Experiment.port = 2553

  val defaults = Array("--verbosity", "0", "--envHomePath", "asdf")

  val intensity = 10

  "MapTest" should "run map successfully." in {
    val conf = new ExperimentConf(
      Array("--algorithms", "map",
            "--chunkSizes", "1",
            "--counts", intensity.toString,
            "--files", "wiki2.xml",
            "--partitions", "1", "4") ++ defaults)

    Experiment.run(conf)

    val berkeleyConf = new ExperimentConf(
      Array("--algorithms", "map",
            "--chunkSizes", "1", "2",
            "--counts", intensity.toString,
            "--store", "berkeleydb",
            "--files", "wiki2.xml",
            "--partitions", "1", "4") ++ defaults)

    Experiment.run(berkeleyConf)
  }

  /*"PageRankTest" should "run page rank successfully." in {
    val conf = Map(
      "algorithms" -> Array("pgrank"),
      "chunkSizes" -> Array("1"),
      "counts" -> Array(intensity.toString),
      "partitions" -> Array("1"))

    Experiment.run(Experiment.confs ++ conf)
  }*/

  /*"WordcountTest" should "run wordcount successfully." in {
    val conf = Map(
      "algorithms" -> Array("wc"),
      "chunkSizes" -> Array("1", "4"),
      "counts" -> Array(intensity.toString),
      "partitions" -> Array("1", "4"))

    Experiment.run(Experiment.confs ++ conf)
  }*/
}

