/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tbd.visualization.graph

import scala.collection.mutable.HashSet

trait GraphIterator extends Iterator[Node] {
  def hasNext(): Boolean
  def next() : Node
}
class DfsIterator(root: Node, graph: Graph, filter: (Edge => Boolean) = (x => true))
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
      //We could throw the order overboard? Will it be a good idea to use lexigraphic ordering? 
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

class TopoSortIterator(root: Node, graph: Graph, filter: (Edge => Boolean) = (x => true))
  extends DfsIterator(root, graph, filter) {

  private var isLast = false

  protected override def lastVisit(n: Node) = { isLast = true }

  override def next(): Node = {
    isLast = false
    while(!isLast) { iterationStep() }
    current
  }
}