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

package thomasdb.visualization.analysis

import scala.math.{min, max}
import thomasdb.visualization.graph._
import scala.collection.mutable.{Buffer, HashSet}

/*
 * Base class for comparisons between DDGs.
 */
abstract class TraceComparison(val extractor: (Node => Any)) {
  def compare(first: DDG, second: DDG): ComparisonResult
}

/*
 * Wraps each node into a structure which uses the given extractor
 * to define node equality.
 */
class NodeWrapper(val node: Node, extractor: (Node => Any)) {

  override def equals(that: Any): Boolean = {
      that.isInstanceOf[NodeWrapper] &&
      extractor(that.asInstanceOf[NodeWrapper].node) == extractor(node)
  }

  override def hashCode(): Int = {
    extractor(node).hashCode()
  }
}

//Represents the result of a trace comparison.
case class ComparisonResult(
  val removed: List[Node],
  val added: List[Node],
  val unchanged:List[Node]) {

  //The trace distance
  def distance() = {
    removed.size + added.size
  }
}
