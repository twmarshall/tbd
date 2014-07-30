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

package tbd.visualization.graph

import scala.collection.mutable.HashSet

trait GraphIterator extends Iterator[Node] {
  def hasNext(): Boolean
  def next() : Node
}
class DfsIterator(
      root: Node,
      graph: Graph,
      filter: (Edge => Boolean) = (x => true))
    extends GraphIterator {
  private val visited = new HashSet[Node]()
  private var state = List((root, 0))

  protected var current: Node = null
  protected def visit(n: Node) = { }
  protected def firstVisit(n: Node) = { }
  protected def lastVisit(n: Node) = { }

  protected def iterationStep() {
    val (head :: tail) = state;
    val (node, index) = head;

    current = node
    visit(node)

    if(index == 0) {
      firstVisit(node)
    }

    val adjList = graph.adj(node)

    if(index < adjList.length) {
      state = (node, index + 1) :: tail
      val nextEdge = adjList(index)

      if(!visited.contains(nextEdge.destination) && filter(nextEdge)) {
        visited += nextEdge.destination
        state = (nextEdge.destination, 0) :: state
      }
    } else {
      lastVisit(node)
      state = tail
    }
  }

  def hasNext(): Boolean = {
    state != List()
  }

  def next(): Node = {
    iterationStep()
    current
  }
}

class TopoSortIterator(
    root: Node,
    graph: Graph,
    filter: (Edge => Boolean) = (x => true))
  extends DfsIterator(root, graph, filter) {

  private var isLast = false

  protected override def lastVisit(n: Node) = { isLast = true }

  override def next(): Node = {
    isLast = false
    while(!isLast) { iterationStep() }
    current
  }
}