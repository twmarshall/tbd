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
package tbd.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import scala.collection.mutable.{ArrayBuffer, Map, MutableList, Set}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tbd.Constants._
import tbd.{Adjustable, Context}
import tbd.ddg._
import tbd.messages._

object Task {
  def props
      (taskId: TaskId,
       parent: ActorRef,
       datastore: ActorRef,
       masterRef: ActorRef): Props =
    Props(classOf[Task], taskId, parent, datastore, masterRef)
}

class Task
    (taskId: TaskId,
     parent: ActorRef,
     datastore: ActorRef,
     masterRef: ActorRef)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val c = new Context(taskId, this, datastore, masterRef)

  def propagate(start: Pointer = Timestamp.MIN_TIMESTAMP,
                end: Pointer = Timestamp.MAX_TIMESTAMP): Future[Boolean] = {
    Future {
      var option = c.ddg.updated.find((timestamp: Pointer) =>
        Timestamp.>(timestamp, start) &&
        Timestamp.<(timestamp, end))
      while (!option.isEmpty) {
        val timestamp = option.get
        c.ddg.updated -= timestamp

        val nodePtr = Timestamp.getNodePtr(timestamp)
        Node.getType(nodePtr) match {
          case Node.ParNodeType =>
            Await.result(Future.sequence(c.pending), DURATION)
            c.pending.clear()

            val future1 =
              c.ddg.getLeftTask(nodePtr) ? PropagateTaskMessage
            val future2 =
                c.ddg.getRightTask(nodePtr) ? PropagateTaskMessage

            ParNode.setPebble1(nodePtr, false)
            ParNode.setPebble2(nodePtr, false)

            Await.result(future1, DURATION)
            Await.result(future2, DURATION)

          case Node.ReadNodeType =>
            val newValue = c.readId(ReadNode.getModId(nodePtr))

            val oldStart = c.reexecutionStart
            c.reexecutionStart = Timestamp.getNext(timestamp)
            val oldEnd = c.reexecutionEnd
            c.reexecutionEnd = Timestamp.getEndPtr(timestamp)
            val oldCurrentModId = c.currentModId
            c.currentModId = Node.getCurrentModId1(nodePtr)
            val oldCurrentModId2 = c.currentModId2
            c.currentModId2 = Node.getCurrentModId2(nodePtr)

            val oldCurrentTime = c.currentTime
            c.currentTime = timestamp

            val reader =
              c.ddg.readers(ReadNode.getReaderId(nodePtr))
            reader(newValue)

            if (Timestamp.<(c.reexecutionStart, c.reexecutionEnd)) {
              c.ddg.splice(c.reexecutionStart, c.reexecutionEnd, c)
            }

            c.reexecutionStart = oldStart
            c.reexecutionEnd = oldEnd
            c.currentModId = oldCurrentModId
            c.currentModId2 = oldCurrentModId2
            c.currentTime = oldCurrentTime

          case Node.Read2NodeType =>
            val newValue1 = c.readId(Read2Node.getModId1(nodePtr))
            val newValue2 = c.readId(Read2Node.getModId2(nodePtr))

            val oldStart = c.reexecutionStart
            c.reexecutionStart = Timestamp.getNext(timestamp)
            val oldEnd = c.reexecutionEnd
            c.reexecutionEnd = Timestamp.getEndPtr(timestamp)
            val oldCurrentModId = c.currentModId
            c.currentModId = Node.getCurrentModId1(nodePtr)
            val oldCurrentModId2 = c.currentModId2
            c.currentModId2 = Node.getCurrentModId2(nodePtr)

            val oldCurrentTime = c.currentTime
            c.currentTime = timestamp

            val reader = c.ddg.read2ers(
              Read2Node.getReaderId(nodePtr))
            reader(newValue1, newValue2)

            if (Timestamp.<(c.reexecutionStart, c.reexecutionEnd)) {
              c.ddg.splice(c.reexecutionStart, c.reexecutionEnd, c)
            }

            c.reexecutionStart = oldStart
            c.reexecutionEnd = oldEnd
            c.currentModId = oldCurrentModId
            c.currentModId2 = oldCurrentModId2
            c.currentTime = oldCurrentTime

          case x => println("unknown node type " + x)
        }

        option = c.ddg.updated.find((timestamp: Pointer) =>
          Timestamp.>(timestamp, start) &&
          Timestamp.<(timestamp, end))
      }

      true
    }
  }

  def receive = {
    case ModUpdatedMessage(modId: ModId) =>
      c.ddg.modUpdated(modId)
      c.updatedMods += modId

      (parent ? PebbleMessage(self, modId)) pipeTo sender

    case RunTaskMessage(adjust: Adjustable[_]) =>
      log.debug("Starting task.")
      sender ! adjust.run(c)
      Await.result(Future.sequence(c.pending), DURATION)
      c.pending.clear()

    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      val newPebble = c.ddg.parUpdated(taskRef, c)

      if (newPebble) {
        (parent ? PebbleMessage(self, modId)) pipeTo sender
      } else {
        sender ! "done"
      }

    case PropagateTaskMessage =>
      log.debug("Running change propagation.")
      c.initialRun = false
      c.epoch += 1

      val respondTo = sender
      val future = propagate()
      future onComplete {
        case Success(t) =>
          c.updatedMods.clear()

          Await.result(Future.sequence(c.pending), DURATION)
          c.pending.clear()

          respondTo ! "done"
        case Failure(e) =>
          e.printStackTrace()
      }

    case GetTaskDDGMessage =>
      sender ! c.ddg

    case ShutdownTaskMessage =>
      val futures = Set[Future[Any]]()
      for ((actorRef, parNode) <- c.ddg.pars) {
        futures += actorRef ? ShutdownTaskMessage
        actorRef ! akka.actor.PoisonPill
      }

      Future.sequence(futures) pipeTo sender

    case x =>
      log.warning("Received unhandled message " + x + " from " + sender)
  }
}
