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

import akka.pattern.ask
import scala.collection.mutable.{HashMap, HashSet, ArrayBuffer}
import scala.concurrent.Await

import thomasdb.Constants._
import thomasdb.visualization.analysis.MethodInfo

/*
 * Represents any graph.
 */
class Graph() {
  val adj = new HashMap[Node, ArrayBuffer[Edge]]()
  val nodes = new HashSet[Node]()
}

/*
 * Represents a DDG.
 *
 * A DDG stripped of all edged except the control edges is guaranteed to be a
 * tree. A DDG has a root.
 */
class DDG(val root: Node) extends Graph {

  //Gets all children of a node, connected by control edges.
  def getCallChildren(node: Node): Seq[Node] = {
    adj(node).filter((e: Edge) => {
      (e.isInstanceOf[Edge.Control])
    }).map(e => e.destination)
  }

  //Gets the parent of a node, connected by an inverse control edge.
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

//Helper object to create visualizer.graph.DDGs from thomasdb.ddg.DDGs.
//This wey we can maintain an independent copy of all necassary information
//for ourselfs, without creating memory leaks or accessing disposed mods.
object DDG {
  //Recursivley creates a visualizer DDG from a ThomasDB DDG.
  def create(ddg: thomasdb.ddg.DDG): DDG = {

    val newNode = new Node(ddg.root)
    val result = new DDG(newNode)

    result.nodes += newNode
    result.adj += (newNode -> new ArrayBuffer[Edge]())

    getChildren(ddg, ddg.startTime, ddg.endTime).foreach(time => {
      append(ddg, newNode, time, result)
    })

    result
  }

  //Fetches child nodes for a given node. Takes extra care of par nodes.
  private def getChildren
      (ddg: thomasdb.ddg.DDG,
       start: thomasdb.ddg.Timestamp,
       end: thomasdb.ddg.Timestamp): Seq[thomasdb.ddg.Timestamp] = {
    start.node match {
      case parNode: thomasdb.ddg.ParNode =>
	val f1 = parNode.taskRef1 ? thomasdb.messages.GetTaskDDGMessage
	val ddg1 = Await.result(f1.mapTo[thomasdb.ddg.DDG], DURATION)
	val f2 = parNode.taskRef2 ? thomasdb.messages.GetTaskDDGMessage
	val ddg2 = Await.result(f2.mapTo[thomasdb.ddg.DDG], DURATION)

        ddg1.ordering.getChildren(ddg1.startTime, ddg1.endTime) ++
        ddg2.ordering.getChildren(ddg2.startTime, ddg2.endTime)
      case _ => ddg.ordering.getChildren(start, end)
    }
  }

  //Recursivley creates a visualizer DDG from a ThomasDB DDG.
  private def append
      (ddg: thomasdb.ddg.DDG,
       node: Node,
       time: thomasdb.ddg.Timestamp,
       result: DDG): Unit = {
    val ddgNode = time.node
    val newNode = new Node(ddgNode)

    result.nodes += newNode
    result.adj += (newNode -> new ArrayBuffer[Edge]())
    result.adj(node) += new Edge.Control(node, newNode)
    result.adj(newNode) += new Edge.InverseControl(newNode, node)

    getChildren(ddg, time, time.end).foreach(x => {
      append(ddg, newNode, x, result)
    })
  }
}
