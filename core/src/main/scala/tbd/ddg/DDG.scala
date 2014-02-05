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

import akka.actor.{Actor, ActorLogging, Props}
import scala.collection.mutable.Map

import tbd.messages._
import tbd.mod.ModId

abstract class Node
case class ReadNode(readId: ReadId, dataIn: List[ModId], dataOut: List[ModId], controlIn: List[ReadId], controlOut: List[ReadId])
case class ModNode(modId: ModId)

object DDG {
  def props(): Props = Props(classOf[DDG])
}

class DDG extends Actor with ActorLogging {
  val reads = Map[ReadId, ReadNode]()

  def addRead(modId: ModId, readId: ReadId) {
    log.debug("Adding read dependency from Mod(" + modId + ") to Read(" + readId + ")")
    val readNode = new ReadNode(readId, List(modId), List(), List(), List())

    reads += (readId -> readNode)
  }

  def addWrite(readId: ReadId, modId: ModId) {
    log.debug("Adding write dependency from Read(" + readId + ") to Mod(" + modId + ")")
    val newNode =
      if (reads.contains(readId)) {
	val oldReadNode = reads(readId)
	reads -= readId
	new ReadNode(readId, oldReadNode.dataIn, modId :: oldReadNode.dataOut,
		     oldReadNode.controlIn, oldReadNode.controlOut)
      } else {
	new ReadNode(readId, List(), List(modId), List(), List())
      }

    reads += (readId -> newNode)
  }

  def addCall(outerCall: ReadId, innerCall: ReadId) {
    log.debug("Adding control dependency from Read(" + outerCall + ") to Read(" + innerCall + ")")

    val newOuterNode =
      if (reads.contains(outerCall)) {
	val oldReadNode = reads(outerCall)
	reads -= outerCall
	new ReadNode(outerCall, oldReadNode.dataIn, oldReadNode.dataOut,
		     oldReadNode.controlIn, innerCall :: oldReadNode.controlOut)
      } else {
	new ReadNode(outerCall, List(), List(), List(), List(innerCall))
      }

    val newInnerNode =
      if (reads.contains(innerCall)) {
	val oldReadNode = reads(innerCall)
	reads -= innerCall
	new ReadNode(innerCall, oldReadNode.dataIn, oldReadNode.dataOut,
		     outerCall :: oldReadNode.controlIn, oldReadNode.controlOut)
      } else {
	new ReadNode(innerCall, List(), List(), List(outerCall), List())
      }

    reads += (outerCall -> newOuterNode)
    reads += (innerCall -> newInnerNode)
  }

  override def toString = {
    reads.map((readId) => readId.toString).reduceLeft(_ + "\n" + _)
  }

  def receive = {
    case AddReadMessage(modId: ModId, readId: ReadId) =>
      addRead(modId, readId)
    case AddWriteMessage(readId: ReadId, modId: ModId) =>
      addWrite(readId, modId)
    case AddCallMessage(outerCall: ReadId, innerCall: ReadId) =>
      addCall(outerCall, innerCall)
    case ToStringMessage => sender ! toString
  }
}
