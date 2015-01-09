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
package thomasdb.examples.list

import thomasdb.list.ListConf

case class AlgorithmConf(
  algorithm: String,
  cacheSize: Int,
  count: Int,
  file: Option[String],
  master: String,
  mutations: List[String],
  runs: List[String],
  repeat: Int,
  store: String,
  listConf: ListConf) {

  def apply(param: String): String =
    param match {
      case "algorithms" => algorithm
      case "counts" => count.toString
    }
}
