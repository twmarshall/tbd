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
package tdb.examples.list

import tdb.list.ListConf

case class AlgorithmConf(
  algorithm: String,
  cacheSize: Int,
  count: Int,
  envHomePath: String,
  file: String,
  master: String,
  mutations: List[String],
  naive: Boolean,
  runs: List[String],
  repeat: Int,
  storeType: String,
  updateFile: String,
  listConf: ListConf,
  iters: Int,
  epsilon: Double) {

  def apply(param: String): String =
    param match {
      case "algorithms" => algorithm
      case "chunkSizes" => listConf.chunkSize.toString
      case "counts" => count.toString
      case "epsilons" => epsilon.toString
      case "files" => file
      case "iters" => iters.toString
      case "partitions" => listConf.partitions.toString
    }
}
