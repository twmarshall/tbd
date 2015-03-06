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
package tdb.list

object ListConf {
  def create[T]
    (file: String = "",
     partitions: Int = 8,
     chunkSize: Int = 1,
     chunkSizer: Any => Int = _ => 1,
     sorted: Boolean = false,
     hash: Boolean = false,
     aggregate: Boolean = false,
     aggregator: (T, T) => T = null,
     deaggregator: (T, T) => T = null,
     initialValue: T): ListConf = {
    new ListConf(
      file,
      partitions,
      chunkSize,
      chunkSizer,
      sorted,
      hash,
      aggregate,
      aggregator.asInstanceOf[(Any, Any) => Any],
      deaggregator.asInstanceOf[(Any, Any) => Any],
      initialValue.asInstanceOf[Any])
  }
}

case class ListConf
    (val file: String = "",
     val partitions: Int = 8,
     chunkSize: Int = 1,
     chunkSizer: Any => Int = _ => 1,
     sorted: Boolean = false,
     hash: Boolean = false,
     aggregate: Boolean = false,
     aggregator: (Any, Any) => Any = null,
     deaggregator: (Any, Any) => Any = null,
     initialValue: Any = null)
