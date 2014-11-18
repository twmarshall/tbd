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
import akka.pattern.ask
import scala.collection.mutable.{Buffer, Map, MutableList, Set, TreeSet}
import scala.concurrent.Await

import tbd._
import tbd.Constants._
import tbd.master.Master
import tbd.messages._

class DDG {
  var root = new RootNode()
  debug.TBD.nodes(root) = (Node.getId(), Tag.Root(), null)

  val reads = Map[ModId, Buffer[Timestamp]]()
  val pars = Map[ActorRef, Timestamp]()

  var updated = TreeSet[Timestamp]()((new TimestampOrdering()).reverse)

  val ordering = new Ordering()

  def addRead
      (mod: Mod[Any],
       value: Any,
       reader: Any => Changeable[Any],
       c: Context): Timestamp = {
    val readNode = new ReadNode(mod, reader)
    val timestamp = nextTimestamp(readNode, c)

    if (reads.contains(mod.id)) {
      reads(mod.id) :+= timestamp
    } else {
      reads(mod.id) = Buffer(timestamp)
    }

    timestamp
  }

  def addRead2
      (mod1: Mod[Any],
       mod2: Mod[Any],
       value1: Any,
       value2: Any,
       reader: (Any, Any) => Changeable[Any],
       c: Context): Timestamp = {
    val readNode = new Read2Node(mod1, mod2, reader)
    val timestamp = nextTimestamp(readNode, c)

    if (reads.contains(mod1.id)) {
      reads(mod1.id) :+= timestamp
    } else {
      reads(mod1.id) = Buffer(timestamp)
    }

    if (reads.contains(mod2.id)) {
      reads(mod2.id) :+= timestamp
    } else {
      reads(mod2.id) = Buffer(timestamp)
    }

    timestamp
  }

  def addMod
      (modId1: ModId,
       modId2: ModId,
       modizer: Modizer[Any],
       key: Any,
       c: Context): Timestamp = {
    val modNode = new ModNode(modId1, modId2, modizer, key)
    val timestamp = nextTimestamp(modNode, c)

    timestamp
  }

  def addWrite[T]
      (mod: Mod[Any],
       mod2: Mod[Any],
       c: Context): Timestamp = {
    val writeNode = new WriteNode(mod, mod2)
    val timestamp = nextTimestamp(writeNode, c)

    timestamp
  }

  def addPar
      (taskRef1: ActorRef,
       taskRef2: ActorRef,
       c: Context): ParNode = {
    val parNode = new ParNode(taskRef1, taskRef2)
    val timestamp = nextTimestamp(parNode, c)

    pars(taskRef1) = timestamp
    pars(taskRef2) = timestamp

    timestamp.end = c.ddg.nextTimestamp(parNode, c)

    parNode
  }

  def addMemo
      (signature: Seq[Any],
       memoizer: Memoizer[_],
       c: Context): Timestamp = {
    val memoNode = new MemoNode(signature, memoizer)
    val timestamp = nextTimestamp(memoNode, c)

    timestamp
  }

  def nextTimestamp(node: Node, c: Context): Timestamp = {
    val time =
      if (c.initialRun)
	ordering.append(node)
      else
	ordering.after(c.currentTime, node)

    c.currentTime = time

    time
  }

  def modUpdated(modId: ModId) {
    for (timestamp <- reads(modId)) {
      if (!timestamp.node.updated) {
        updated += timestamp

	timestamp.node.updated = true
      }
    }
  }

  // Pebbles a par node. Returns true iff the pebble did not already exist.
  def parUpdated(taskRef: ActorRef): Boolean = {
    val timestamp = pars(taskRef)
    val parNode = timestamp.node.asInstanceOf[ParNode]

    if (!parNode.pebble1 && !parNode.pebble2) {
      updated += timestamp
      parNode.updated = true
    }

    if (parNode.taskRef1 == taskRef) {
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
   * Replaces all of the mods equal to mod1 with mod2 in the subtree rooted at
   * node. This is called when a memo match is made where the memo node has a
   * currentMod that's different from the Context's currentMod.
   */
  def replaceMods(_timestamp: Timestamp, _node: Node, mod1: Mod[Any], mod2: Mod[Any]): Buffer[Node] = {
    val buf = Buffer[Node]()

    var time = _timestamp
    while (time < _timestamp.end) {
      val node = time.node

      if (time.end != null) {
        if (node.currentMod == mod1) {

	  buf += time.node
	  process(node, mod1, mod2)

	  node.currentMod = mod2
	} else if (node.currentMod2 == mod1) {
          if (time.end != null) {
	    buf += time.node
	    process(node, mod1, mod2)

	    node.currentMod2 = mod2
	  }
        } else {
          // Skip the subtree rooted at this node since this node doesn't have
          // mod1 as its currentMod and therefore none of its children can either.
	  time = time.end
        }
      }

      time = time.getNext()
    }

    buf
  }

  private def process(node: Node, mod1: Mod[Any], mod2: Mod[Any]) {
    node match {
      case memoNode: MemoNode =>
	memoNode.value match {
	  case changeable: Changeable[_] =>
	    if (changeable.mod == mod1) {
	      changeable.asInstanceOf[Changeable[Any]].mod = mod2
	    }
	  case (c1: Changeable[_], c2: Changeable[_]) =>
	    if (c1.mod == mod1) {
	      c1.asInstanceOf[Changeable[Any]].mod = mod2
	    }

	    if (c2.mod == mod1) {
	      c2.asInstanceOf[Changeable[Any]].mod = mod2
	    }
	  case _ =>
	}
      case _ =>
    }
  }

  def startTime = ordering.base.next.base

  def endTime = ordering.base.base

  override def toString = toString("")

  def toString(prefix: String): String = {
    val out = new StringBuffer("")
    def innerToString(time: Timestamp, prefix: String) {
      val thisString = time.node match {
	case memo: MemoNode =>
	  prefix + memo + " time = " + time + " to " + time.end +
	  " signature=" + memo.signature + "\n"
	case mod: ModNode =>
	  prefix + mod + " time = " + time + " to " + time.end + "\n"
	case par: ParNode =>
	  val future1 = par.taskRef1 ? GetTaskDDGMessage
	  val future2 = par.taskRef2 ? GetTaskDDGMessage

	  val ddg1 = Await.result(future1.mapTo[DDG], DURATION)
	  val ddg2 = Await.result(future2.mapTo[DDG], DURATION)

	  prefix + par + " pebbles=(" + par.pebble1 +
	  ", " + par.pebble2 + ")\n" + ddg1.toString(prefix + "|") +
	  ddg2.toString(prefix + "|")
	case read: ReadNode =>
	  prefix + read + " modId=(" + read.mod.id + ") " + " time=" +
	  time + " to " + time.end + " value=" + read.mod +
	  " updated=(" + read.updated + ")\n"
	case root: RootNode =>
	  prefix + "RootNode=" + root + "\n"
	case write: WriteNode =>
	  prefix + write + " modId=(" + write.mod.id + ") " +
	  " value=" + write.mod + "\n"
	case x => "???"
      }
      out.append(thisString)

      for (time <- ordering.getChildren(time, time.end)) {
	innerToString(time, prefix + "-")
      }
    }
    innerToString(startTime.getNext(), prefix)

    out.toString
  }
}
