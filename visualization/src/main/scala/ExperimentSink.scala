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

trait ExperimentSource[T, V] {

  var listener: ExperimentSink[T, V]

  def setDDGListener(listener: ExperimentSink[T, V]) = {
    this.listener = listener
  }

  def pushResult(result: ExperimentResult[T, V]) = {
    if(listener != null) {
      listener.resultReceived(result, this)
    }
  }
}


trait ExperimentSink[T, V] {
  def resultReceived(result: ExperimentResult[T, V], sender: ExperimentSource[T, V])
}

case class ExperimentResult[+T, +V](
  val runId: Int,
  val input: V,
  val result: T,
  val expectedResult: T,
  val ddg: graph.DDG) { }