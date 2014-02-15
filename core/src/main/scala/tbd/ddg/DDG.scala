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

object DDG {
  var lastId = 0

  def getId(): Int = {
    lastId += 1
    lastId
  }
}

class DDG(log: LoggingAdapter) {
  val id = DDG.getId()

  var root = new RootNode(id)
  val reads = Map[ModId, Set[ReadNode[Any, Any]]]()
  val pars = Map[ActorRef, ParNode]()

  implicit val order = scala.math.Ordering[Double]
    .on[Node](_.timestamp.time)
  var updated = PriorityQueue[Node]()

  val ordering = new Ordering()

  def addRead[T, U](
      mod: Mod[Any],
      aParent: Node,
      reader: T => Changeable[U]): Node = {
    val parent =
      if (aParent == null) {
	      root
      } else {
	      aParent
      }

    val timestamp = nextTimestamp(parent)
    val readNode = new ReadNode(mod, parent, timestamp, reader)
    parent.addChild(readNode)
    

    if (reads.contains(mod.id)) {
      reads(mod.id) += readNode.asInstanceOf[ReadNode[Any, Any]]
    } else {
      reads(mod.id) = Set(readNode.asInstanceOf[ReadNode[Any, Any]])
    }

    readNode
  }

  def addWrite(mod: Mod[Any], aParent: Node): Node = {
    val parent =
      if (aParent == null) {
	      root
      } else {
	      aParent
      }

    val timestamp = nextTimestamp(parent)
    val writeNode = new WriteNode(mod, parent, timestamp)

    parent.addChild(writeNode)

    writeNode
  }

  def addPar(workerRef1: ActorRef, workerRef2: ActorRef, aParent: Node) {
    val parent =
      if (aParent == null) {
	      root
      } else {
	      aParent
      }

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
    for (readNode <- reads(modId)) {
      readNode.updated = true
      updated += readNode
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(workerRef: ActorRef): Boolean = {
    val parNode = pars(workerRef)
    updated += parNode
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
    node.children.clear()
  }

  override def toString = {
    root.toString("")
  }

  def toString(prefix: String): String = {
    root.toString(prefix)
  }
}
