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

import scala.collection.mutable.{HashMap, HashSet, ArrayBuffer}

class Graph() {
  val adj = new HashMap[Node, ArrayBuffer[Edge]]()
  val nodes = new HashSet[Node]()
}

class DDG(_root: Node) extends Graph {
  val root = _root

  def getCallChildren(node: Node): Seq[Node] = {
    adj(node).filter((e: Edge) => {
      (e.isInstanceOf[Edge.Control])
    }).map(e => e.destination)
  }

  def getCallParent(node: Node): Node = {
    val parent = adj(node).filter(e => {
      e.isInstanceOf[Edge.InverseControl]
    }).headOption
    if(parent.isEmpty) {
      null
    } else {
      parent.get.destination
    }
  }
}

object DDG {
  def create(root: tbd.ddg.RootNode): DDG = {

    val newNode = new Node(root)
    val result = new DDG(newNode)

    result.nodes += newNode
    result.adj += (newNode -> new ArrayBuffer[Edge]())

    getChildren(root).foreach(x => {
      append(newNode, x, result)
    })

    result
  }

  private def getChildren(node: tbd.ddg.Node): Seq[tbd.ddg.Node] = {
    node match {
      case parNode: tbd.ddg.ParNode =>
        Seq(parNode.getFirstSubtree().root.children(0),
        parNode.getSecondSubtree().root.children(0))
      case _ => node.children
    }
  }

  private def append(node: Node, ddgNode: tbd.ddg.Node, result: DDG): Unit = {
    val newNode = new Node(ddgNode)

    result.nodes += newNode
    result.adj += (newNode -> new ArrayBuffer[Edge]())
    result.adj(node) += new Edge.Control(node, newNode)
    result.adj(newNode) += new Edge.InverseControl(newNode, node)

    getChildren(ddgNode).foreach(x => {
      append(newNode, x, result)
    })
  }
}