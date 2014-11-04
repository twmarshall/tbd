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
package tbd.examples.test

import org.scalatest._

import tbd.examples.list.Experiment

class AlgorithmTests extends FlatSpec with Matchers {
  Experiment.repeat = 0
  Experiment.verbosity = 0
  Experiment.check = true
  Experiment.port = 2553

  val intensity = 10

  "FilterTest" should "run filter successfully." in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("filter"),
					   "chunkSizes" -> Array("1"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1", "4")))
  }

  "FlatMapTest" should "run flatMap successfully" in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("flatMap"),
					   "chunkSizes" -> Array("1", "4"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1", "4")))
  }

  "JoinTest" should "run join successfully." in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("join"),
					   "chunkSizes" -> Array("1", "4"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1")))

    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("sjoin"),
					   "chunkSizes" -> Array("1"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1")))
  }

  /*"MapTest" should "run map successfully." in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("map"),
					   "chunkSizes" -> Array("1", "4"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1", "4")))
  }*/

  "PageRankTest" should "run page rank successfully." in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("pgrank"),
					   "chunkSizes" -> Array("1"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1")))
  }

  "ReduceByKeyTest" should "run reduceByKey successfully" in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("rbk"),
					   "chunkSizes" -> Array("1", "4"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1")))
  }

  "SortTest" should "run sort successfully." in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("qsort"),
					   "chunkSizes" -> Array("1"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1", "4")))

    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("msort"),
					   "chunkSizes" -> Array("1", "2"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1", "4")))
  }

  /*"SplitTest" should "run split successfully." in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("split"),
					   "chunkSizes" -> Array("1"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1")))
  }*/

  /*"WordcountTest" should "run wordcount successfully." in {
    Experiment.run(Experiment.confs ++ Map("algorithms" -> Array("wc"),
					   "chunkSizes" -> Array("1", "4"),
					   "counts" -> Array(intensity.toString),
					   "partitions" -> Array("1", "4")))
  }*/
}
