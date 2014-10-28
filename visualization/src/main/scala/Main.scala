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

package tbd.visualization

import tbd.visualization.analysis._
import org.rogach.scallop._
import scala.language.existentials


object Main {
  //Main entry point.
  def main(args: Array[String]) {

    object Conf extends ScallopConf(args) {
      version("TBD 0.1 (c) 2014 Carnegie Mellon University")
      banner("Usage: visualizer.sh -a algorithm [options]\n")
      val algo = opt[String]("algorithm", 'a', required = true,
        descr = "The algorithm to run: quicksort, reduce, split, map")
      val initialCount = opt[Int]("initialCount", 'i', default = Some(10),
        descr = "The count of elements in initial input data")
      val mutationRoundCount = opt[Int]("mutationRoundCount", 'c',
        default = Some(10), descr = "The count of mutations rounds to run")
      val maxMutations = opt[Int]("maximalMutationsPerPropagation", 'p',
        default = Some(5), descr = "The count of maximal mutations " +
        "(insert, update, delete) per mutation round")
      val minMutations = opt[Int]("minimalMutationsPerPropagation", 'k',
        default = Some(0),
        descr = "The count of minimal mutations per mutation round")
      val output = opt[String]("o", 'o', default = Some("visualizer"),
        descr = "Sets the output mode: visualizer (default), diff, latex or 2dplot.")
      val testmode = opt[String]("test", 't', default = Some("random"),
        descr = "Test case generation mode: random (default), manual or exhaustive")
    }

    //Creates the ExperimentSource for the selected test.
    def createTestEnvironment[T, V](algo: TestAlgorithm[T, V]) = {
      Conf.testmode.get.get match {
        case "manual" => new ManualTest(algo) {
          initialSize = Conf.initialCount.get.get
        }

        case "random" => new RandomExhaustiveTest(algo) {
          maxMutations = Conf.maxMutations.get.get
          minMutations = Conf.minMutations.get.get
          count = Conf.mutationRoundCount.get.get
          initialSize = Conf.initialCount.get.get
        }

        case "exhaustive" => new TargetedExhaustiveTest(algo) {
          initialSize = Conf.initialCount.get.get
        }
      }
    }

    //Creates the ExperimentSink for processing the experiment results.
    def createOutput[V](): ExperimentSink[V] = {
      Conf.output.get.get match {
        case "visualizer" => new SingleView[V]() {
          visible = true
        }
        case "diff" => new DiffView[V](){
          visible = true
        }
        case "chart2d" => new UpdateLengthPositionPlot[V](
          new analysis.GreedyTraceComparison((node => node.tag))
        )
        case "latex" => new LatexExport[V]()
      }
    }

    def create[T, V](algo: TestAlgorithm[T, V]) = {
      val test = createTestEnvironment(algo)
      val output = createOutput[V]()
      new Main(test, List(output))
    }

    //Creates the test algorithm.
    val main = Conf.algo.get.get match {
      case "reduce" => create(new ListReduceSumTest())
      case "sort" => create(new ListSortTest())
      case "split" => create(new ListSplitTest())
      case "map" => create(new ListMapTest())
      case "modDependency" => create(new ModDepTest())
      case "wc" => create(new WordCountTest())
    }

    main.run()
  }
}

/*
 * Runs a given test, tracks all dependencies, and then sends the result
 * to the given ExperimentSink.
 */
class Main[T, V](val test: TestBase[T, V],
                 val outputs: List[ExperimentSink[V]])
    extends ExperimentSink[V] {
  test.setExperimentListener(this)

  val modTracker = new ModDependencyTracker()
  val freeVarTracker = new FreeVarDependencyTracker()
  val readWriteTracker = new ReadWriteDependencyTracker()

  def resultReceived(
    result: ExperimentResult[V],
    sender: ExperimentSource[V]) = {
      modTracker.findAndInsertDependencies(result.ddg)
      freeVarTracker.findAndInsertDependencies(result.ddg)
      readWriteTracker.findAndInsertDependencies(result.ddg)
      outputs.foreach(_.resultReceived(result, sender))
  }

  def run() {
    test.run()
    outputs.foreach(x => x.finish())
  }
}
