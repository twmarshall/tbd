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
import scala.collection.mutable.{Buffer, Map, MutableList, Set, TreeSet}

import tbd._
import tbd.Constants._
import tbd.master.Master

class DDG(id: String) {
  var root = new RootNode(id)
  root.tag = Tag.Root()

  val reads = Map[ModId, Buffer[ReadNode]]()
  val pars = Map[ActorRef, ParNode]()

  var updated = TreeSet[Node]()((new TimestampOrdering()).reverse)

  val ordering = new Ordering()

  def addRead
      (mod: Mod[Any],
       value: Any,
       parent: Node,
       reader: Any => Changeable[Any],
       initialRun: Boolean): ReadNode = {
    val timestamp =
      if (initialRun)
	ordering.append()
      else
	nextTimestamp(parent)

    val readNode = new ReadNode(mod, parent, timestamp, reader)

    parent.addChild(readNode)

    if (reads.contains(mod.id)) {
      reads(mod.id) :+= readNode.asInstanceOf[ReadNode]
    } else {
      reads(mod.id) = Buffer(readNode.asInstanceOf[ReadNode])
    }

    readNode
  }

  def addMod
      (parent: Node,
       modizer: Modizer[Any],
       key: Any,
       initialRun: Boolean): ModNode = {
    val timestamp =
      if (initialRun)
	ordering.append()
      else
	nextTimestamp(parent)

    val modNode = new ModNode(modizer, key, parent, timestamp)

    parent.addChild(modNode)

    modNode
  }

  def addWrite[T]
      (mod: Mod[Any],
       mod2: Mod[Any],
       parent: Node,
       initialRun: Boolean): WriteNode = {
    val timestamp =
      if (initialRun)
	ordering.append()
      else
	nextTimestamp(parent)

    val tag = if(tbd.master.Main.debug) {
      val writes = List(mod, mod2).filter(_ != null).map((x: Mod[Any]) => {
        SingleWriteTag(x.id, x.read())
      }).toList
      Tag.Write(writes)
    } else {
      null
    }

    val writeNode = new WriteNode(mod, mod2, parent, timestamp)
    writeNode.tag = tag

    parent.addChild(writeNode)

    writeNode
  }

  def addPar
      (workerRef1: ActorRef,
       workerRef2: ActorRef,
       parent: Node,
       initialRun: Boolean): ParNode = {
    val timestamp =
      if (initialRun)
	ordering.append()
      else
	nextTimestamp(parent)

    val parNode = new ParNode(workerRef1, workerRef2, parent, timestamp)

    parent.addChild(parNode)

    pars(workerRef1) = parNode
    pars(workerRef2) = parNode

    parNode
  }

  def addMemo
      (parent: Node,
       signature: Seq[Any],
       memoizer: Memoizer[_],
       initialRun: Boolean): MemoNode = {
    val timestamp =
      if (initialRun)
	ordering.append()
      else
	nextTimestamp(parent)

    val memoNode = new MemoNode(parent, timestamp, signature, memoizer)

    parent.addChild(memoNode)
    memoNode
  }

  def nextTimestamp(parent: Node): Timestamp = {
    if (parent.children.size == 0) {
      ordering.after(parent.timestamp)
    } else {
      ordering.after(parent.children.last.endTime)
    }
  }

  def modUpdated(modId: ModId) {
    for (readNode <- reads(modId)) {
      if (!readNode.updated) {
        updated += readNode

	readNode.updated = true
      }
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(workerRef: ActorRef): Boolean = {
    val parNode = pars(workerRef)
    if (!parNode.pebble1 && !parNode.pebble2) {
      updated += parNode
      parNode.updated = true
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
   * Called before a read is reexecuted, the descendents of this node are
   * cleaned up, up to the first memo nodes, which are returned so that
   * they can be reattached or cleaned up later.
   */
  def cleanupRead(subtree: Node): MutableList[Node] = {
    val ret = new MutableList[Node]()

    for (child <- subtree.children) {
      child.parent = null
    }

    ret ++= subtree.children

    subtree.children.clear()

    ret
  }

  def cleanupSubtree(subtree: Node) {
    for (child <- subtree.children) {
      cleanupSubtree(child)
    }

    cleanup(subtree)
  }

  private def cleanup(node: Node) {
    node match {
      case readNode: ReadNode =>
	reads(readNode.mod.id) -= readNode
      case memoNode: MemoNode =>
	memoNode.memoizer.removeEntry(memoNode.timestamp, memoNode.signature)
      case modNode: ModNode =>
	if (modNode.modizer != null) {
	  modNode.modizer.remove(modNode.key)
	}
      case _ =>
    }

    node.updated = false
    ordering.remove(node.timestamp)

    node.children.clear()
  }

  def attachSubtree(parent: Node, subtree: Node) {
    if (subtree.parent != null) {
      subtree.parent.removeChild(subtree)

      var oldParent = subtree.parent
      while (oldParent != null) {
        oldParent.matchableInEpoch = Master.epoch + 1
        oldParent = oldParent.parent
      }
    }

    parent.addChild(subtree)
    subtree.parent = parent
  }

  /**
   * Replaces all of the mods equal to mod1 with mod2 in the subtree rooted at
   * node. This is called when a memo match is made where the memo node has a
   * currentMod that's different from the Context's currentMod.
   */
  def replaceMods(node: Node, mod1: Mod[Any], mod2: Mod[Any]) {
    if (node.isInstanceOf[MemoNode]) {
      val memoNode = node.asInstanceOf[MemoNode]
      if (memoNode.value.isInstanceOf[Changeable[_]]) {
	val changeable = memoNode.value.asInstanceOf[Changeable[Any]]
	if (changeable.mod == mod1) {
	  changeable.mod = mod2
	}
      }

      if (memoNode.value.isInstanceOf[Tuple2[_, _]]) {
        val tuple = memoNode.value.asInstanceOf[Tuple2[Any, Any]]

        if (tuple._1.isInstanceOf[Changeable[_]]) {
	  val changeable = tuple._1.asInstanceOf[Changeable[Any]]
	  if (changeable.mod == mod1) {
	    changeable.mod = mod2
	  }
        }

        if (tuple._2.isInstanceOf[Changeable[_]]) {
	  val changeable = tuple._2.asInstanceOf[Changeable[Any]]
	  if (changeable.mod == mod1) {
	    changeable.mod = mod2
	  }
        }
      }
    }

    if (node.currentMod == mod1) {
      node.currentMod = mod2

      // Because reads and memos must have as their currentMod the mod
      // that is closest in enclosing scope, a node that doesn't have mod1 as
      // its mod can't have any children that have mod1 either.
      for (child <- node.children) {
	replaceMods(child, mod1, mod2)
      }
    } else if(node.currentMod2 == mod1) {
      node.currentMod2 = mod2

      for (child <- node.children) {
	replaceMods(child, mod1, mod2)
      }
    }
  }

  override def toString = {
    root.toString("")
  }

  def toString(prefix: String): String = {
    root.toString(prefix)
  }
}
