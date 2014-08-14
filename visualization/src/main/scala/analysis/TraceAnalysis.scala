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

package tbd.visualization.analysis

import tbd.visualization.ExperimentResult

abstract class TraceAnalysis {
  def analyze(trace: ExperimentResult[Any]): Iterable[AnalysisInfo]
}

class AnalysisInfo(val message: String, val file: String, val position: Int) { }
case class IndependentSubtreeInfo(
    override val message: String,
    override val file: String,
    val position1: Int,
    val position2: Int)
  extends AnalysisInfo(message, file, position1)
