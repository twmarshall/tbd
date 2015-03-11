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
  def apply[T]
    (file: String = "",
     partitions: Int = 0,
     chunkSize: Int = 1,
     chunkSizer: Any => Int = _ => 1,
     sorted: Boolean = false,
     hash: Boolean = false): ListConf = {
    new SimpleListConf(
      file,
      partitions,
      chunkSize,
      chunkSizer,
      sorted,
      hash)
  }
}

sealed trait ListConf {
  def file: String
  def partitions: Int
  def chunkSize: Int
  def chunkSizer: Any => Int
  def sorted: Boolean
  def hash: Boolean

  def clone
    (file: String = file,
     partitions: Int = partitions): ListConf
}

case class SimpleListConf
    (file: String = "",
     partitions: Int = 0,
     chunkSize: Int = 1,
     chunkSizer: Any => Int = _ => 1,
     sorted: Boolean = false,
     hash: Boolean = false) extends ListConf {

  def clone(_file: String, _partitions: Int) =
    copy(file = _file, partitions = _partitions)
}

case class AggregatorListConf[T]
    (file: String = "",
     partitions: Int = 0,
     chunkSize: Int = 1,
     chunkSizer: Any => Int = _ => 1,
     sorted: Boolean = false,
     hash: Boolean = false,
     aggregator: (T, T) => T,
     deaggregator: (T, T) => T,
     initialValue: T,
     threshold: T => Boolean) extends ListConf {
  def clone(_file: String, _partitions: Int) =
    copy(file = _file, partitions = _partitions)
}

case class ColumnListConf
    (file: String = "",
     partitions: Int = 0,
     chunkSize: Int = 1,
     chunkSizer: Any => Int = _ => 1,
     sorted: Boolean = false,
     hash: Boolean = false,
     columns: Map[String, (ColumnType, Any)]) extends ListConf {

  assert(columns.contains("key"))

  def clone(_file: String, _partitions: Int) =
    copy(file = _file, partitions = _partitions)
}
