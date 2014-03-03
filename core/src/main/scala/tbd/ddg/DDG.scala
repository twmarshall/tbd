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
package tbd.ddg

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import scala.collection.mutable.{Map, PriorityQueue, Set}

import tbd.Changeable
import tbd.mod.{Mod, ModId}

class DDG(log: LoggingAdapter, id: String) {
  var root = new RootNode(id)
  val reads = Map[ModId, Set[ReadNode]]()
  val pars = Map[ActorRef, ParNode]()

  implicit val order = scala.math.Ordering[Double]
    .on[Node](_.timestamp.time).reverse
  var updated = PriorityQueue[Node]()

  val ordering = new Ordering()

  def addRead(
      mod: Mod[Any],
      parent: Node,
      reader: Any => Changeable[Any]): Node = {
    val timestamp = nextTimestamp(parent)
    val readNode = new ReadNode(mod, parent, timestamp, reader)
    parent.addChild(readNode)

    if (reads.contains(mod.id)) {
      reads(mod.id) += readNode.asInstanceOf[ReadNode]
    } else {
      reads(mod.id) = Set(readNode.asInstanceOf[ReadNode])
    }

    readNode
  }

  def addWrite(mod: Mod[Any], parent: Node): Node = {
    val timestamp = nextTimestamp(parent)
    val writeNode = new WriteNode(mod, parent, timestamp)

    parent.addChild(writeNode)

    writeNode
  }

  def addPar(workerRef1: ActorRef, workerRef2: ActorRef, parent: Node) {
    val timestamp = nextTimestamp(parent)

    val parNode = new ParNode(workerRef1, workerRef2, parent, timestamp)
    parent.addChild(parNode)

    pars(workerRef1) = parNode
    pars(workerRef2) = parNode
  }

  private def nextTimestamp(parent: Node): Timestamp = {
    if (parent.children.size == 0) {
      ordering.after(parent.timestamp)
    } else {
      ordering.after(parent.children.last.timestamp)
    }
  }

  def modUpdated(modId: ModId) {
    if (reads.contains(modId)) {
      for (readNode <- reads(modId)) {
        if (!readNode.updated) {
          updated += readNode
        }
        readNode.updated = true
      }
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(workerRef: ActorRef): Boolean = {
    val parNode = pars(workerRef)
    if (!parNode.pebble1 && !parNode.pebble2) {
      updated += parNode
    }

    if (parNode.workerRef1 == workerRef) {
      val ret = !parNode.pebble1
      parNode.pebble1 = true
      ret
    } else {
      val ret = !parNode.pebble2
      parNode.pebble2 = true
      ret
    }
  }

  def removeSubtree(node: Node) {
    for (child <- node.children) {
      if (child.isInstanceOf[ReadNode]) {
        reads -= child.asInstanceOf[ReadNode].mod.id
      }

      updated = updated.filter((node2: Node) => child != node2)
      removeSubtree(child)
    }

    node.children.clear()
  }

  override def toString = {
    root.toString("")
  }

  def toString(prefix: String): String = {
    root.toString(prefix)
  }
}
