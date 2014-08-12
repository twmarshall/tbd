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

import scala.collection.immutable.Map

trait ExperimentSource[T] {

  var listener: ExperimentSink[T]

  def setDDGListener(listener: ExperimentSink[T]) = {
    this.listener = listener
  }

  def pushResult(result: ExperimentResult[T]) = {
    if(listener != null) {
      listener.resultReceived(result, this)
    }
  }
}

trait ExperimentSink[T] {
  def resultReceived(result: ExperimentResult[T],
                     sender: ExperimentSource[T])

  def finish() = { }
}

case class ExperimentResult[+T](
  val runId: Int,
  val input: Map[Int, Int],
  val mutations: List[Mutation],
  val result: T,
  val expectedResult: T,
  val ddg: graph.DDG) { }