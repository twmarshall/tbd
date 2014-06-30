/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tbd.visualization.analysis

import scala.math.{min, max}
import tbd.visualization.graph._
import scala.collection.mutable.Buffer

object TraceComparison {
  def MonotoneTraceDistance(before: DDG, after: DDG): ComparisonResult = {
    val a = new TopoSortIterator(
              before.root,
              before,
              (e: Edge) => (e.edgeType == EdgeType.Call)).toBuffer

    val b = new TopoSortIterator(
              after.root,
              after,
              (e: Edge) => (e.edgeType == EdgeType.Call)).toBuffer
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
}

class ComparisonResult(_removed: List[Node], _added: List[Node], _unchanged:List[Node]) {
  val removed = _removed
  val added = _added
  val unchanged = _unchanged
}
