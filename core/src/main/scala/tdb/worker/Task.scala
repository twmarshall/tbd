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
package tdb.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import java.io.BufferedWriter
import scala.collection.mutable.{ArrayBuffer, Map, MutableList, Set}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tdb.Constants._
import tdb.{Adjustable, Context}
import tdb.ddg._
import tdb.messages._
import tdb.stats.WorkerStats

object Task {
  def props
      (taskId: TaskId,
       mainDatastoreId: TaskId,
       parent: ActorRef,
       masterRef: ActorRef,
       datastores: Map[TaskId, ActorRef]): Props =
    Props(classOf[Task], taskId, mainDatastoreId, parent, masterRef, datastores)
}

class Task
    (taskId: TaskId,
     mainDatastoreId: TaskId,
     parent: ActorRef,
     masterRef: ActorRef,
     datastores: Map[TaskId, ActorRef])
  extends Actor with ActorLogging {
  import context.dispatcher

  WorkerStats.numTasks += 1

  private val c = new Context(
    taskId, mainDatastoreId, self, masterRef, datastores, log)

  def receive = {
    case ModUpdatedMessage(modId: ModId) =>
      c.ddg.modUpdated(modId)
      c.updatedMods += modId

      (parent ? PebbleMessage(self, modId)) pipeTo sender

    case NodeUpdatedMessage(nodeId: NodeId) =>
      c.ddg.nodeUpdated(nodeId)

      (parent ? PebbleMessage(self, -1)) pipeTo sender

    case ModRemovedMessage(modId: ModId) =>
      c.ddg.modRemoved(modId)
      sender ! "done"

    case KeyUpdatedMessage(inputId: InputId, key: Any) =>
      val newPebble = c.ddg.updated.size == 0

      c.ddg.keyUpdated(inputId, key)

      if (newPebble) {
        (parent ? PebbleMessage(self, -1)) pipeTo sender
      } else {
        sender ! "done"
      }

    case KeyRemovedMessage(inputId: InputId, key: Any) =>
      c.ddg.keyRemoved(inputId, key)
      sender ! "done"

    case RunTaskMessage(adjust: Adjustable[_]) =>
      log.debug("Starting task.")
      val ret = adjust.run(c)
      for ((input, buf) <- c.buffers) {
        buf.flush()
      }
      for ((input, buf) <- c.bufs) {
        buf.flush()
      }
      c.ddg.root.end = c.ddg.nextTimestamp(c.ddg.root.node, c)

      sender ! ret
      Await.result(Future.sequence(c.pending), DURATION)
      c.pending.clear()

    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      val newPebble = c.ddg.parUpdated(taskRef)

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
      val future = c.propagate()
      future onComplete {
        case Success(t) =>
          c.updatedMods.clear()

          Await.result(Future.sequence(c.pending), DURATION)
          c.pending.clear()

          for ((input, buf) <- c.buffers) {
            buf.flush()
          }
          for ((input, buf) <- c.bufs) {
            buf.flush()
          }

          respondTo ! "done"
        case Failure(e) =>
          e.printStackTrace()
      }

    case GetTaskDDGMessage =>
      sender ! c.ddg

    case PrintDDGDotsMessage(nextName: Int, output: BufferedWriter) =>
      sender ! (new DDGPrinter(c.ddg, nextName, output)).print()

    case ClearModsMessage =>
      val futures = Set[Future[Any]]()

      futures += datastores(c.mainDatastoreId) ? RemoveModsMessage(c.ddg.getMods(), self)

      for ((actorRef, parNode) <- c.ddg.pars) {
        futures += actorRef ? ClearModsMessage
      }

      Future.sequence(futures) pipeTo sender

    case ShutdownTaskMessage =>
      self ! akka.actor.PoisonPill

      WorkerStats.numTasks -= 1

      val futures = Set[Future[Any]]()
      for ((actorRef, parNode) <- c.ddg.pars) {
        futures += actorRef ? ShutdownTaskMessage
      }

      Future.sequence(futures) pipeTo sender

    case x =>
      log.warning("Received unhandled message " + x + " from " + sender)
  }
}
