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

object Main {
  def main(args: Array[String]) {

    val main = new Main(new ExhaustiveTest(new ListQuicksortTest()))
    main.run()

    //val test = new ExhaustiveTest(new ListMapTest())
    //test.setDDGListener() //Get that shit to work.
    //val test = new ManualTest(new ListReduceSumTest())
    //Possible Options:
    //test.showDDGEachStep = true
    //test.initialSize = 2
    //test.maximalMutationsPerPropagation = 1

    //test.run()
  }
}

class Main[T, V](val test: TestBase[T, V]) extends ExperimentSink[V, Seq[Int]] {

    test.setDDGListener(this)

    val mainView = new MainView()
    mainView.visible = true

    //val export = new LatexExport()

    def resultReceived(
      result: ExperimentResult[V, Seq[Int]],
      sender: ExperimentSource[V, Seq[Int]]) = {
      mainView.addResult(result)

      //println(export.export(result.ddg))

      readLine()
    }

    def run() {
      test.run()
    }
}
