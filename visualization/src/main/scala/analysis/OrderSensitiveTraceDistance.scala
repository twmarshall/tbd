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

/*
 * Order sensitive trace distance.
 * Practically not very useful.
 */
class OrderSensitiveTraceComparison(extractor: (Node => Any))
    extends TraceComparison(extractor) {
  def compare(before: DDG, after: DDG): ComparisonResult = {

    //Sort all nodes of both DDGs in topological order.
    val a = new TopoSortIterator(
              before.root,
              before,
              (e: Edge) => (e.isInstanceOf[Edge.Control])).map({
                x => new NodeWrapper(x, extractor)
              }).toBuffer

    val b = new TopoSortIterator(
              after.root,
              after,
              (e: Edge) => (e.isInstanceOf[Edge.Control])).map({
                x => new NodeWrapper(x, extractor)
              }).toBuffer

    //The toposort iterates in inverse order,
    //however we concat the result in reverse order when backtracking, too
    //so the result is correct.

    //Now, compute a (levensthein) editing distance on the two ordered sets.
    val m = new Array[Array[Int]](a.length + 1)

    for(i <- 0 to a.length) {
      m(i) = new Array[Int](b.length + 1)
      for(j <- 0 to b.length) {
        m(i)(j) =
          if(min(i, j) == 0) {
            max(i, j)
          } else {
            val equal = if(a(i - 1) == b(j - 1)) {
              m(i - 1)(j - 1)
            } else {
              Int.MaxValue
            }
            val add = m(i)(j - 1) + 1
            val remove = m(i - 1)(j) + 1

            min(min(equal, add), remove)
          }
      }
    }

    //Backtrack changes to see which nodes were removed, and which were kept. 
    backtrackChanges(m, a, b, a.length, b.length,
                         List[Node](), List[Node](), List[Node]())
  }

  private def backtrackChanges(
        m: Array[Array[Int]],
        a: Buffer[NodeWrapper],
        b: Buffer[NodeWrapper],
        i: Int,
        j: Int,
        removed: List[Node],
        added: List[Node],
        unchanged: List[Node]):
      ComparisonResult = {

    if(i > 0 && m(i - 1)(j) + 1 == m(i)(j)) {
      backtrackChanges(m, a, b, i - 1, j,
                       a(i - 1).node :: removed, added, unchanged)
    } else if(j > 0 && m(i)(j - 1) + 1 == m(i)(j)) {
      backtrackChanges(m, a, b, i, j - 1,
                       removed, b(j - 1).node :: added, unchanged)
    } else if(i > 0 && j > 0) {
      backtrackChanges(m, a, b, i - 1, j - 1,
                       removed, added, a(i - 1).node :: unchanged)
    } else {
      new ComparisonResult(removed, added, unchanged)
    }

  }
}