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
  def main(args: Array[String]) {

    //Set debug flag to true so we can have nice tags
    tbd.master.Main.debug = true

    object Conf extends ScallopConf(args) {
      version("TBD Visualizer 0.1 (c) 2014 Carnegie Mellon University")
      banner("Usage: visualizer.sh -a [ALGORITHM] [OPTIONS]\n" +
             "Help: visualizer.sh --help")
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
        descr = "Sets the output mode: visualizer (default), diff or 2dplot.")
      val testmode = opt[String]("test", 't', default = Some("random"),
        descr = "Test case generation mode: random (default), manual or exhaustive")
    }

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

    def createOutput[V](): ExperimentSink[V] = {
      Conf.output.get.get match {
        case "visualizer" => new MainView[V](false) {
          visible = true
        }
        case "diff" => new MainView[V](true){
          visible = true
        }
        case "chart2d" => new UpdateLengthPositionPlot[V](new analysis.GreedyTraceComparison((node => node.tag)))
      }
    }

    def create[T, V](algo: TestAlgorithm[T, V]) = {
      val test = createTestEnvironment(algo)
      val output = createOutput[V]()
      new Main(test, List(output))
    }

    val main = Conf.algo.get.get match {
      case "reduce" => create(new ListReduceSumTest())
      case "quicksort" => create(new ListQuicksortTest())
      case "split" => create(new ListSplitTest())
      case "map" => create(new ListMapTest())
      case "modDependency" => create(new ModDepTest())
    }

    main.run()
  }
}

class Main[T, V](val test: TestBase[T, V],
                 val outputs: List[ExperimentSink[V]])
    extends ExperimentSink[V] {
  test.setDDGListener(this)

  //val export = new LatexExport()

  def resultReceived(
    result: ExperimentResult[V],
    sender: ExperimentSource[V]) = {
      new ModDependencyTracker().findAndInsertDependencies(result.ddg)
      new FreeVarDependencyTracker().findAndInsertDependencies(result.ddg)
      new ReadWriteDependencyTracker().findAndInsertDependencies(result.ddg)
      outputs.foreach(_.resultReceived(result, sender))
  }

  def run() {
    test.run()
    outputs.foreach(x => x.finish())
  }
}
