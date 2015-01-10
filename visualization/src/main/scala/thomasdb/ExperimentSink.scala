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

package thomasdb.visualization

import scala.collection.immutable.Map

/*
 * Trait for a class which generates results for a given algorithm.
 */
trait ExperimentSource[T] {

  var listener: ExperimentSink[T]

  //Attaches a listener.
  def setExperimentListener(listener: ExperimentSink[T]) = {
    this.listener = listener
  }

  //Pushes the given ExperimentResult to the attached listener.
  protected def pushResult(result: ExperimentResult[T]) = {
    if(listener != null) {
      listener.resultReceived(result, this)
    }
  }
}

/*
 * Trait for a class which receives ExperimentResults from an ExperimentSource
 */
trait ExperimentSink[T] {

  //Called whenever the ExperimentSource generates a new ExperimentResult.
  def resultReceived(result: ExperimentResult[T],
                     sender: ExperimentSource[T])

  //Called when the ExperimentSource finished generating results.
  def finish() = { }
}

/*
 * Represents the result of an initial run or change propagation.
 */
case class ExperimentResult[+T](
  val runId: Int, //The unique id of the run.
  val input: Map[Int, Int], //The input set of the run.
  val mutations: List[Mutation], //The input mutations from the previous to this run.
  val result: T, //The result of this run, from the incremental algorithm.
  val expectedResult: T, //The expected result of this run, from the test code.
  val ddg: graph.DDG) //The ddg of this run.
