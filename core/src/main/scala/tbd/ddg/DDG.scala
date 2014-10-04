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
       reader: Any => Changeable[Any],
       c: Context): ReadNode = {
    val readNode = new ReadNode(mod, reader)
    val timestamp = nextTimestamp(c.currentParent, readNode, c)
    readNode.timestamp = timestamp

    c.currentParent.addChild(readNode)

    if (reads.contains(mod.id)) {
      reads(mod.id) :+= readNode.asInstanceOf[ReadNode]
    } else {
      reads(mod.id) = Buffer(readNode.asInstanceOf[ReadNode])
    }

    readNode
  }

  def addMod
      (modizer: Modizer[Any],
       key: Any,
       c: Context): ModNode = {
    val modNode = new ModNode(modizer, key)
    val timestamp = nextTimestamp(c.currentParent, modNode, c)
    modNode.timestamp = timestamp

    c.currentParent.addChild(modNode)

    modNode
  }

  def addWrite[T]
      (mod: Mod[Any],
       mod2: Mod[Any],
       c: Context): WriteNode = {
    val writeNode = new WriteNode(mod, mod2)
    val timestamp = nextTimestamp(c.currentParent, writeNode, c)
    writeNode.timestamp = timestamp

    val tag = if(tbd.master.Main.debug) {
      val writes = List(mod, mod2).filter(_ != null).map((x: Mod[Any]) => {
        SingleWriteTag(x.id, x.read())
      }).toList
      Tag.Write(writes)
    } else {
      null
    }

    writeNode.tag = tag

    c.currentParent.addChild(writeNode)

    writeNode
  }

  def addPar
      (workerRef1: ActorRef,
       workerRef2: ActorRef,
       c: Context): ParNode = {
    val parNode = new ParNode(workerRef1, workerRef2)
    val timestamp = nextTimestamp(c.currentParent, parNode, c)
    parNode.timestamp = timestamp

    c.currentParent.addChild(parNode)

    pars(workerRef1) = parNode
    pars(workerRef2) = parNode

    parNode
  }

  def addMemo
      (signature: Seq[Any],
       memoizer: Memoizer[_],
       c: Context): MemoNode = {
    val memoNode = new MemoNode(signature, memoizer)
    val timestamp = nextTimestamp(c.currentParent, memoNode, c)
    memoNode.timestamp = timestamp

    c.currentParent.addChild(memoNode)
    memoNode
  }

  def nextTimestamp(parent: Node, node: Node, c: Context): Timestamp = {
    val time =
      if (c.initialRun)
	ordering.append(node)
      else
	ordering.after(c.currentTime, node)

    c.currentTime = time

    time
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

  def attachSubtree(parent: Node, subtree: Node) {
    parent.addChild(subtree)
  }

  /**
   * Replaces all of the mods equal to mod1 with mod2 in the subtree rooted at
   * node. This is called when a memo match is made where the memo node has a
   * currentMod that's different from the Context's currentMod.
   */
  def replaceMods(_node: Node, mod1: Mod[Any], mod2: Mod[Any]): Buffer[Node] = {
    val buf = Buffer[Node]()

    var time = _node.timestamp
    while (time < _node.endTime) {
      val node = time.node

      if (node.currentMod == mod1) {
	if (node.timestamp == time) {

	  buf += time.node
	  process(node, mod1, mod2)

	  node.currentMod = mod2
	}
      } else if (node.currentMod2 == mod1) {
	if (node.timestamp == time) {
	  buf += time.node
	  process(node, mod1, mod2)

	  node.currentMod2 = mod2
	}
      } else {
	time = node.endTime
      }

      time = time.getNext()
    }

    buf
  }

  private def process(node: Node, mod1: Mod[Any], mod2: Mod[Any]) {
    node match {
      case memoNode: MemoNode =>
	memoNode.value match {
	  case changeable: Changeable[Any] =>
	    if (changeable.mod == mod1) {
	      changeable.mod = mod2
	    }
	  case (c1: Changeable[Any], c2: Changeable[Any]) =>
	    if (c1.mod == mod1) {
	      c1.mod = mod2
	    }

	    if (c2.mod == mod1) {
	      c2.mod = mod2
	    }
	  case _ =>
	}
      case _ =>
    }
  }

  override def toString = {
    root.toString("")
  }

  def toString(prefix: String): String = {
    root.toString(prefix)
  }
}
