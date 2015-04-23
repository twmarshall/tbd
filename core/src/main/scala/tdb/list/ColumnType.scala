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

sealed trait ColumnType

case class AggregatedColumn
    (columnType: String,
     aggregator: (Any, Any) => Any,
     deaggregator: (Any, Any) => Any,
     initialValue: Any,
     threshold: Any => Boolean) extends ColumnType

object AggregatedColumn {
  def create[T]
      (columnType: String,
       aggregator: (T, T) => T,
       deaggregator: (T, T) => T,
       initialValue: T,
       threshold: T => Boolean) = {
    AggregatedColumn(
      columnType,
      aggregator.asInstanceOf[(Any, Any) => Any],
      deaggregator.asInstanceOf[(Any, Any) => Any],
      initialValue.asInstanceOf[Any],
      threshold.asInstanceOf[Any => Boolean])
  }
}

object AggregatedDoubleColumn {
  def apply(epsilon: Double = 0) =
    AggregatedColumn.create(
      "Double",
      aggregator = ((_: Double) + (_: Double)),
      deaggregator = ((_: Double) - (_: Double)),
      initialValue = 0.0,
      threshold = ((_: Double).abs > epsilon))
}

object AggregatedIntColumn {
  def apply(epsilon: Double = 0) =
    AggregatedColumn.create(
      "Int",
      aggregator = (_: Int) + (_: Int),
      deaggregator = (_: Int) - (_: Int),
      initialValue = 0,
      threshold = (_: Int).abs > epsilon)
}

case class StringColumn() extends ColumnType
