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
import akka.pattern.ask
import scala.collection.mutable.{ArrayBuffer, Map, MutableList, Set}
import scala.concurrent.{Await, Future, Promise}
import scala.util.Try

import tbd.Constants._
import tbd.{Adjustable, Context}
import tbd.ddg.{DDG, Node, MemoNode, ParNode, ReadNode, Timestamp}
import tbd.messages._

object Worker {
  def props(id: String, datastoreRef: ActorRef, parent: ActorRef): Props =
    Props(classOf[Worker], id, datastoreRef, parent)
}

class Worker(val id: String, val datastoreRef: ActorRef, parent: ActorRef)
  extends Actor with ActorLogging {
  import context.dispatcher

  val ddg = new DDG(log, id, this)

  private val c = new Context(id, this)

  def propagate(start: Timestamp = Timestamp.MIN_TIMESTAMP,
                end: Timestamp = Timestamp.MAX_TIMESTAMP): Future[Boolean] = {
    Future {
      var option = ddg.updated.find((node: Node) =>
        node.timestamp > start && node.timestamp < end)
      while (!option.isEmpty) {
        val node = option.get
        ddg.updated -= node

        if (node.updated) {
          if (node.isInstanceOf[ReadNode]) {
            val readNode = node.asInstanceOf[ReadNode]

            val toCleanup = ddg.cleanupRead(readNode)

            val newValue = readNode.mod.read()

	    val oldCurrentParent = c.currentParent
            c.currentParent = readNode
	    val oldStart = c.reexecutionStart
	    c.reexecutionStart = readNode.timestamp
	    val oldEnd = c.reexecutionEnd
	    c.reexecutionEnd = readNode.endTime
            val oldCurrentDest = c.currentMod
            c.currentMod = readNode.currentMod
	    val oldCurrentDest2 = c.currentMod2
	    c.currentMod2 = readNode.currentMod2

            readNode.updated = false
            readNode.reader(newValue)

	    c.currentParent = oldCurrentParent
	    c.reexecutionStart = oldStart
	    c.reexecutionEnd = oldEnd
            c.currentMod = oldCurrentDest
	    c.currentMod2 = oldCurrentDest2

            for (node <- toCleanup) {
              if (node.parent == null) {
                ddg.cleanupSubtree(node)
              }
            }
          } else {
            val parNode = node.asInstanceOf[ParNode]

            Await.result(Future.sequence(c.pending), DURATION)
            c.pending.clear()

            val future1 = parNode.workerRef1 ? PropagateMessage
            val future2 = parNode.workerRef2 ? PropagateMessage

            parNode.pebble1 = false
            parNode.pebble2 = false

            Await.result(future1, DURATION)
            Await.result(future2, DURATION)
          }
        }

        option = ddg.updated.find((node: Node) =>
          node.timestamp > start && node.timestamp < end)
      }

      true
    }

  }

  def receive = {
    case ModUpdatedMessage(modId: ModId, finished: Future[_]) => {
      ddg.modUpdated(modId)
      c.updatedMods += modId

      parent ! PebbleMessage(self, modId, finished)
    }

    case RunTaskMessage(adjust: Adjustable[_]) => {
      sender ! adjust.run(c)
      Await.result(Future.sequence(c.pending), DURATION)
      c.pending.clear()
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId, finished: Promise[String]) => {
      val newPebble = ddg.parUpdated(workerRef)

      if (newPebble) {
        parent ! PebbleMessage(self, modId, finished)
      } else {
        finished.success("done")
      }
    }

    case PropagateMessage => {
      c.initialRun = false
      val respondTo = sender
      val future = propagate()
      future.onComplete((t: Try[Boolean]) => {
	c.updatedMods.clear()

        Await.result(Future.sequence(c.pending), DURATION)
        c.pending.clear()

        respondTo ! "done"
      })
    }

    case GetDDGMessage => {
      sender ! ddg
    }

    case DDGToStringMessage(prefix: String) => {
      sender ! ddg.toString(prefix)
    }

    case CleanupWorkerMessage => {
      val futures = Set[Future[Any]]()
      for ((actorRef, parNode) <- ddg.pars) {
        futures += actorRef ? CleanupWorkerMessage
        actorRef ! akka.actor.PoisonPill
      }

      for (future <- futures) {
        Await.result(future, DURATION)
      }

      sender ! "done"
    }

    case x => {
      log.warning(id + " received unhandled message " + x + " from " + sender)
    }
  }
}
