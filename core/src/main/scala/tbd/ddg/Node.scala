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
import scala.collection.mutable.MutableList
import scala.concurrent.Await

import tbd.Changeable
import tbd.Constants._
import tbd.master.Main
import tbd.messages._
import tbd.mod.{Mod, AsyncMod}

abstract class Node(_parent: Node, _timestamp: Timestamp) {
  var parent = _parent
  val timestamp: Timestamp = _timestamp
  var endTime: Timestamp = null
  var stacktrace =
    if (Main.debug)
      Thread.currentThread().getStackTrace()
    else
      null

  var children = MutableList[Node]()

  var updated = false

  // The earliest epoch in which this node may be matched, if it is a MemoNode.
  // This is increased above the current epoch whenever the node is matched, so
  // that it won't be matched again in this round of change propagation.
  var matchableInEpoch = 0

  def addChild(child: Node) {
    children += child
  }

  def removeChild(child: Node) {
    children = children.filter(_ != child)
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[Node] && obj.asInstanceOf[Node].timestamp == timestamp
  }

  def toString(prefix: String): String = {
    if (children.isEmpty) {
	    ""
    } else if (children.size == 1) {
	    "\n" + children.head.toString(prefix + "-")
    } else {
      var ret = ""
      for (child <- children) {
        ret += "\n" + child.toString(prefix + "-")
      }
      ret
    }
  }
}

class ReadNode(
    _mod: Mod[Any],
    _parent: Node,
    _timestamp: Timestamp,
    _reader: Any => Changeable[Any])
      extends Node(_parent, _timestamp) {
  val mod: Mod[Any] = _mod
  val reader = _reader

  override def toString(prefix: String) = {
    prefix + this + " modId=(" + mod.id + ") " + " time=" + timestamp + " to " + endTime +
      " value=" + mod + " updated=(" + updated + ")" + super.toString(prefix)
  }
}

class WriteNode(_mod: Mod[Any], _parent: Node, _timestamp: Timestamp)
    extends Node(_parent, _timestamp) {
  val mod: Mod[Any] = _mod

  override def toString(prefix: String) = {
    prefix + "WriteNode modId=(" + mod.id + ") " +
      " value=" + mod + " time=" + timestamp + super.toString(prefix)
  }
}

class AsyncNode(
    _asyncWorkerRef: ActorRef,
    _parent: Node,
    _timestamp: Timestamp,
    _mod: AsyncMod[Any]) extends Node(_parent, _timestamp) {

  val asyncWorkerRef = _asyncWorkerRef
  var pebble = false

  def invalidate() {
    _mod.invalidate()
  }

  override def toString(prefix: String) = {
    val future = asyncWorkerRef ? DDGToStringMessage(prefix + "|")
    val output = Await.result(future, DURATION).asInstanceOf[String]

    prefix + "LazyNode time=" + timestamp + "\n" + output + super.toString(prefix)
  }
}

class ParNode(
    _workerRef1: ActorRef,
    _workerRef2: ActorRef,
    _parent: Node,
    _timestamp: Timestamp) extends Node(_parent, _timestamp) {
  val workerRef1 = _workerRef1
  val workerRef2 = _workerRef2

  var pebble1 = false
  var pebble2 = false

  override def toString(prefix: String) = {
    val future1 = workerRef1 ? DDGToStringMessage(prefix + "|")
    val future2 = workerRef2 ? DDGToStringMessage(prefix + "|")

    val output1 = Await.result(future1, DURATION).asInstanceOf[String]
    val output2 = Await.result(future2, DURATION).asInstanceOf[String]

    prefix + "ParNode time=" + timestamp + " pebbles=(" + pebble1 + ", " +
      pebble2 + ")\n" + output1 + "\n" + output2 + super.toString(prefix)
  }
}

class MemoNode(
    _parent: Node,
    _timestamp: Timestamp,
    _signature: List[Any]) extends Node(_parent, _timestamp) {
  val signature = _signature

  override def toString(prefix: String) = {
    prefix + "MemoNode time=" + timestamp + " to " + endTime + " signature=" + signature +
      super.toString(prefix)
  }
}

class RootNode(id: String) extends Node(null, null) {
  override def toString(prefix: String) = {
    prefix + "RootNode id=(" + id + ")" + super.toString(prefix)
  }
}
