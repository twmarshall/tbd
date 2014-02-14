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
import akka.util.Timeout
import scala.collection.mutable.{PriorityQueue, Set}
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.Changeable
import tbd.messages._
import tbd.mod.ModId

abstract class Node(aParent: Node, aTimestamp: Timestamp) {
  val parent = aParent
  var timestamp: Timestamp = aTimestamp

  val children = Set[Node]()

  def addChild(child: Node) {
    children += child
  }

  def removeChild(child: Node) {
    children -= child
  }

   def toString(prefix: String): String = {
    if (children.isEmpty) {
	    ""
    } else if (children.size == 1) {
	    "\n" + children.head.toString(prefix + "-")
    } else {
      implicit val order = scala.math.Ordering[Double]
        .on[Node](_.timestamp.time).reverse
      var updated = PriorityQueue[Node]()
      for (child <- children) {
        updated += child
      }

      var ret = ""
      for (child <- updated) {
        ret += "\n" + child.toString(prefix + "-")
      }
      ret
    }
  }
}

class ReadNode[T](aModId: ModId, aParent: Node, aTimestamp: Timestamp, aReader: T => Changeable[T])
    extends Node(aParent, aTimestamp) {
  val modId: ModId = aModId
  var updated = false
  val reader = aReader

  override def toString(prefix: String) = {
    prefix + "ReadNode mod=(" + modId +") time=" + timestamp + " updated=(" +
      updated + ")" + super.toString(prefix)
  }
}

class WriteNode(aModId: ModId, aParent: Node, aTimestamp: Timestamp)
    extends Node(aParent, aTimestamp) {
  val modId: ModId = aModId

  override def toString(prefix: String) = {
    prefix + "WriteNode mod=(" + modId + ") time=" + timestamp +
      super.toString(prefix)
  }
}

class ParNode(
    aWorkerRef1: ActorRef,
    aWorkerRef2: ActorRef,
    aParent: Node,
    aTimestamp: Timestamp) extends Node(aParent, aTimestamp) {
  val workerRef1 = aWorkerRef1
  val workerRef2 = aWorkerRef2

  var pebble1 = false
  var pebble2 = false

  override def toString(prefix: String) = {
    implicit val timeout = Timeout(5 seconds)
    val future1 = workerRef1 ? DDGToStringMessage(prefix + "|")
    val future2 = workerRef2 ? DDGToStringMessage(prefix + "|")

    val output1 = Await.result(future1, timeout.duration).asInstanceOf[String]
    val output2 = Await.result(future2, timeout.duration).asInstanceOf[String]

    prefix + "ParNode time=" + timestamp + " pebbles=(" + pebble1 + ", " +
      pebble2 + ")\n" + output1 + "\n" + output2 + super.toString(prefix)
  }
}

class RootNode(id: Int) extends Node(null, null) {
  override def toString(prefix: String) = {
    prefix + "RootNode id=(" + id + ")" + super.toString(prefix)
  }
}
