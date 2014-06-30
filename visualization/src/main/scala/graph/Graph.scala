/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package tbd.visualization.graph

import scala.collection.mutable.{HashMap, HashSet, ArrayBuffer}

class Graph() {
  val adj = new HashMap[Node, ArrayBuffer[Edge]]()
  val nodes = new HashSet[Node]()
}

class DDG(_root: Node) extends Graph {
  val root = _root
}

object DDG {
  def create(root: tbd.ddg.RootNode): DDG = {

    val newNode = new Node(root)
    val result = new DDG(newNode)

    result.nodes += newNode
    result.adj += (newNode -> new ArrayBuffer[Edge]())

    root.children.foreach(x => {
      append(newNode, x, result)
    })

    result
  }

  private def append(node: Node, ddgNode: tbd.ddg.Node, result: DDG): Unit = {
    val newNode = new Node(ddgNode)

    result.nodes += newNode
    result.adj += (newNode -> new ArrayBuffer[Edge]())
    result.adj(node) += new Edge(EdgeType.Call, node, newNode)

    ddgNode.children.foreach(x => {
      append(newNode, x, result)
    })
  }
}