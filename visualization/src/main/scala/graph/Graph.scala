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

import akka.pattern.ask
import scala.collection.mutable.{HashMap, HashSet, ArrayBuffer}
import scala.concurrent.Await

import tbd.Constants._
import tbd.visualization.analysis.MethodInfo

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

//Helper object to create visualizer.graph.DDGs from tbd.ddg.DDGs.
//This wey we can maintain an independent copy of all necassary information
//for ourselfs, without creating memory leaks or accessing disposed mods.
object DDG {
  //Recursivley creates a visualizer DDG from a TBD DDG.
  def create(ddg: tbd.ddg.DDG): DDG = {

    val newNode = new Node(ddg.startTime)

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
      (ddg: tbd.ddg.DDG,
       start: tbd.ddg.Timestamp,
       end: tbd.ddg.Timestamp): Seq[tbd.ddg.Timestamp] = {
    tbd.ddg.Node.getType(start.nodePtr) match {
      case tbd.ddg.Node.ParNodeType =>
        println("!!")
        val f1 = ddg.getLeftTask(start.nodePtr) ? tbd.messages.GetTaskDDGMessage
        val ddg1 = Await.result(f1.mapTo[tbd.ddg.DDG], DURATION)
        val f2 =
          ddg.getRightTask(start.nodePtr) ? tbd.messages.GetTaskDDGMessage
        val ddg2 = Await.result(f2.mapTo[tbd.ddg.DDG], DURATION)

        ddg1.getChildren(ddg1.startTime, ddg1.endTime) ++
        ddg2.getChildren(ddg2.startTime, ddg2.endTime)
      case _ => ddg.getChildren(start, end)
    }
  }

  //Recursivley creates a visualizer DDG from a TBD DDG.
  private def append
      (ddg: tbd.ddg.DDG,
       node: Node,
       time: tbd.ddg.Timestamp,
       result: DDG): Unit = {
    val newNode = new Node(time)

    result.nodes += newNode
    result.adj += (newNode -> new ArrayBuffer[Edge]())
    result.adj(node) += new Edge.Control(node, newNode)
    result.adj(newNode) += new Edge.InverseControl(newNode, node)

    getChildren(ddg, time, time.end).foreach(x => {
      append(ddg, newNode, x, result)
    })
  }
}
