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

import scala.math.{min, max}
import tbd.visualization.graph._
import scala.collection.mutable.{Buffer, HashSet}

object TraceComparison {
  def orderSensitiveTraceDistance(before: DDG, after: DDG): ComparisonResult = {
    val a = new TopoSortIterator(
              before.root,
              before,
              (e: Edge) => (e.isInstanceOf[Edge.Control])).toBuffer

    val b = new TopoSortIterator(
              after.root,
              after,
              (e: Edge) => (e.isInstanceOf[Edge.Control])).toBuffer
    //Theoretically, the toposort has the wrong order,
    //however we concat the result in reverse order when backtracking, too
    //so the result is correct.

    val m = new Array[Array[Int]](a.length + 1)

    for(i <- 0 to a.length) {
      m(i) = new Array[Int](b.length + 1)
      for(j <- 0 to b.length) {
        m(i)(j) =
          if(min(i, j) == 0) {
            max(i, j)
          } else {
            val equal = if(a(i - 1) ~ b(j - 1)) m(i - 1)(j - 1) else Int.MaxValue
            val add = m(i)(j - 1) + 1
            val remove = m(i - 1)(j) + 1

            min(min(equal, add), remove)
          }
      }
    }

    println("Distance: " + m(a.length)(b.length))

    backtrackChanges(m, a, b, a.length, b.length,
                         List[Node](), List[Node](), List[Node]())
  }

  private def backtrackChanges(m: Array[Array[Int]],
                               a: Buffer[Node], b: Buffer[Node],
                               i: Int, j: Int, removed: List[Node],
                               added: List[Node], unchanged: List[Node]): ComparisonResult = {

    if(i > 0 && m(i - 1)(j) + 1 == m(i)(j)) {
      backtrackChanges(m, a, b, i - 1, j,
                       a(i - 1) :: removed, added, unchanged)
    } else if(j > 0 && m(i)(j - 1) + 1 == m(i)(j)) {
      backtrackChanges(m, a, b, i, j - 1,
                       removed, b(j - 1) :: added, unchanged)
    } else if(i > 0 && j > 0) {
      backtrackChanges(m, a, b, i - 1, j - 1,
                       removed, added, a(i - 1) :: unchanged)
    } else {
      new ComparisonResult(removed, added, unchanged)
    }

  }

  def greedyTraceDistance(before: DDG, after: DDG): ComparisonResult = {

    var set = after.nodes.map(x => new NodeWrapper(x))

    var removed = List[Node]()
    var added = List[Node]()
    var unchanged = List[Node]()

    before.nodes.map(x => new NodeWrapper(x)).foreach(x => {
      if(set.remove(x)) {
        unchanged = x.node :: unchanged
      } else {
        added = x.node :: added
      }
    })

    set.foreach(x => removed = x.node :: removed)

    new ComparisonResult(removed, added, unchanged)
  }
}

class ComparisonResult(val removed: List[Node],val added: List[Node], val unchanged:List[Node]) {
}

class NodeWrapper(val node: Node) {

  override def equals(that: Any): Boolean = {
      that.isInstanceOf[NodeWrapper] &&
      that.asInstanceOf[NodeWrapper].node.tag == node.tag
  }

  override def hashCode(): Int = {
    node.tag.hashCode()
  }
}