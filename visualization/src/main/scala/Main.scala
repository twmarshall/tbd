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

import tbd.visualization.analysis.DependencyTracker
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
      val diff = opt[Boolean]("diff", 'd', default = Some(false),
        descr = "Activate diff mode. The visualizer will show two graphs " +
        "side-by-side to calculate and visualize trace distance")
      val manual = opt[Boolean]("manual", 'm', default = Some(false),
        descr = "Activate manual mode. The command line can be used to " +
        "modify the input")
    }

    def createTestEnvironment[T, V](algo: TestAlgorithm[T, V]) = {
      if(Conf.manual.get.get) {
        new ManualTest(algo) {
          initialSize = Conf.initialCount.get.get
        }
      } else {
        new ExhaustiveTest(algo) {
          maxMutations = Conf.maxMutations.get.get
          minMutations = Conf.minMutations.get.get
          count = Conf.mutationRoundCount.get.get
          initialSize = Conf.initialCount.get.get
        }
      }
    }

    def create[T, V](algo: TestAlgorithm[T, V]) = {
      val test = createTestEnvironment(algo)
      val mainView = new MainView(Conf.diff.get.get)
      new Main(test, mainView)
    }

    val main = Conf.algo.get.get match {
      case "reduce" => create(new ListReduceSumTest())
      case "quicksort" => create(new ListQuicksortTest())
      case "split" => create(new ListSplitTest())
      case "map" => create(new ListMapTest())
    }

    main.run()
  }
}

class Main[T, V](val test: TestBase[T, V], val mainView: MainView)
    extends ExperimentSink[V, Seq[Int]] {
  test.setDDGListener(this)

  mainView.visible = true

  //val export = new LatexExport()

  def resultReceived(
    result: ExperimentResult[V, Seq[Int]],
    sender: ExperimentSource[V, Seq[Int]]) = {
      DependencyTracker.findAndInsertReadWriteDependencies(result.ddg)
      DependencyTracker.findAndInsertFreeVarDependencies(result.ddg)
      DependencyTracker.findAndInsertModWriteDependencies(result.ddg)
      mainView.addResult(result)
  }

  def run() {
    test.run()
  }
}
