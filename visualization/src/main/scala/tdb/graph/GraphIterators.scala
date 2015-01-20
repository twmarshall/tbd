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

package thomasdb.visualization.graph

import scala.collection.mutable.HashSet

/*
 * Trait for graph iterators, so we can iterate a graph in different, meaningful
 * ways.
 */
trait GraphIterator extends Iterator[Node] {
  //True, iff there is a next element to iterate over.
  def hasNext(): Boolean
  //Moves the iterator to the next element and returns it.
  def next() : Node
}

/*
 * A DFS iterator, which is queue-based.
 * Iterates a graph in depth-first order.
 */
class DfsIterator(
      //The node to start iteration from.
      root: Node,
      //The graph to iterate.
      graph: Graph,
      //An optional filter function to include only edges we want to consult
      //for this iteration.
      filter: (Edge => Boolean) = (x => true))
    extends GraphIterator {

  private val visited = new HashSet[Node]()
  private var state = List((root, 0))

  protected var current: Node = null

  //Called whenever a node is visited.
  protected def visit(n: Node) = { }
  //Called whenever a node is visited for the first time.
  protected def firstVisit(n: Node) = { }
  //Called whenever a node is visited for the last time.
  protected def lastVisit(n: Node) = { }

  //Moves to the next node in the graph.
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

/*
 * Iterates a graph in topological order: All nodes are iterated exactly once,
 * when they are visited the last time by the DFS.
 */
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

/*
 * Iteratesa all nodes exactly once, when they are visited the first time
 * by the DFS.
 */
class DfsFirstIterator(
    root: Node,
    graph: Graph,
    filter: (Edge => Boolean) = (x => true))
  extends DfsIterator(root, graph, filter) {

  private var isFirst = false

  protected override def firstVisit(n: Node) = { isFirst = true }

  //It is difficult to provide a "hasNext" for this kind of iteration,
  //so we just pre-fetch the next node every time. If we don't find a next node
  //any more, we can be sure that we don't have a next node.

  private var nextNode: Node = null
  moveToNext()

  override def next(): Node = {
    val res = nextNode
    moveToNext()
    res
  }

  override def hasNext(): Boolean = {
    nextNode != null
  }

  //Find and remember next node. 
  private def moveToNext() {
    isFirst = false
    while(!isFirst && super.hasNext()) { iterationStep() }
    if(isFirst) {
      nextNode = current
    } else {
      nextNode = null
    }
  }
}
