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
import tbd.worker.Worker

class DDG(log: LoggingAdapter, id: String, worker: Worker) {
  var root = new RootNode(id)
  val reads = Map[ModId, Set[ReadNode]]()
  val pars = Map[ActorRef, ParNode]()

  var updated = PriorityQueue[Node]()(new TimestampOrdering())

  val ordering = new Ordering()

  var lastRemovedMemo: MemoNode = null

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

  def addMemo(parent: Node, signature: List[Any]): Node = {
    val timestamp = nextTimestamp(parent)
    val memoNode = new MemoNode(parent, timestamp, signature)
    parent.addChild(memoNode)
    memoNode
  }

  private def nextTimestamp(parent: Node): Timestamp = {
    if (parent.children.size == 0) {
      ordering.after(parent.timestamp)
    } else {
      nextTimestamp(parent.children.last)
    }
  }

  def modUpdated(modId: ModId) {
    assert(reads.contains(modId))
    for (readNode <- reads(modId)) {
      if (!readNode.updated) {
        updated += readNode
      }
      readNode.updated = true
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

  /**
   * Removes Nodes and their Timestamps and children from this graph, starting
   * at 'subtree'. If 'saveMemo' is true, MemoNodes and their decendants are
   * not cleaned up but instead returned in a Set so that the Worker can clean
   * them up later
   */
  def removeSubtree(subtree: Node, saveMemo: Boolean): Set[Node] = {
    val ret = Set[Node]()
    def innerRemoveSubtree(node: Node) {
      if (node.isInstanceOf[MemoNode] && saveMemo) {
        node.parent.removeChild(node)
        node.parent = null
        ret += node
      } else {
        if (node.isInstanceOf[ReadNode]) {
          val readNode = node.asInstanceOf[ReadNode]
          reads(readNode.mod.id) -= readNode
        } else if (node.isInstanceOf[MemoNode]) {
          worker.memoTable -= node.asInstanceOf[MemoNode].signature
        }

        updated = updated.filter((node2: Node) => node != node2)
        ordering.remove(node.timestamp)

        for (child <- node.children) {
          innerRemoveSubtree(child)
        }

        node.children.clear()
      }
    }

    if (subtree.children.size > 0) {
      for (child <- subtree.children) {
        innerRemoveSubtree(child)
      }

      subtree.children.clear()
    }

    ret
  }

  def attachSubtree(parent: Node, subtree: Node) {
    if (subtree.parent != null) {
      subtree.parent.removeChild(subtree)
    }

    parent.addChild(subtree)
    subtree.parent = parent
  }

  override def toString = {
    root.toString("")
  }

  def toString(prefix: String): String = {
    root.toString(prefix)
  }
}
