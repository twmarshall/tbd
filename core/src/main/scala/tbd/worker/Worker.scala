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
import scala.util.{Failure, Success, Try}

import tbd.Constants._
import tbd.{Adjustable, Context}
import tbd.ddg.{DDG, Node, MemoNode, ParNode, ReadNode, Timestamp}
import tbd.messages._

object Worker {
  def props(id: String, parent: ActorRef): Props =
    Props(classOf[Worker], id, parent)
}

class Worker(val id: String, parent: ActorRef)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val c = new Context(id, this)

  def propagate(start: Timestamp = Timestamp.MIN_TIMESTAMP,
                end: Timestamp = Timestamp.MAX_TIMESTAMP): Future[Boolean] = {
    Future {
      var option = c.ddg.updated.find((node: Node) =>
        node.timestamp > start && node.timestamp < end)
      while (!option.isEmpty) {
        val node = option.get
        c.ddg.updated -= node

        if (node.updated) {
          if (node.isInstanceOf[ReadNode]) {
            val readNode = node.asInstanceOf[ReadNode]

            readNode.children.clear()

            val newValue = readNode.mod.read()

	    val oldCurrentParent = c.currentParent
            c.currentParent = readNode
	    val oldStart = c.reexecutionStart
	    c.reexecutionStart = readNode.timestamp.getNext()
	    val oldEnd = c.reexecutionEnd
	    c.reexecutionEnd = readNode.endTime
            val oldCurrentDest = c.currentMod
            c.currentMod = readNode.currentMod
	    val oldCurrentDest2 = c.currentMod2
	    c.currentMod2 = readNode.currentMod2

	    val oldCurrentTime = c.currentTime
	    c.currentTime = readNode.timestamp

            readNode.updated = false
            readNode.reader(newValue)

	    if (c.reexecutionStart < c.reexecutionEnd) {
	      c.ddg.ordering.splice(c.reexecutionStart, c.reexecutionEnd, c)
	    }

	    c.currentParent = oldCurrentParent
	    c.reexecutionStart = oldStart
	    c.reexecutionEnd = oldEnd
            c.currentMod = oldCurrentDest
	    c.currentMod2 = oldCurrentDest2
	    c.currentTime = oldCurrentTime
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

        option = c.ddg.updated.find((node: Node) =>
          node.timestamp > start && node.timestamp < end)
      }

      true
    }
  }

  def receive = {
    case ModUpdatedMessage(modId: ModId, finished: Future[_]) => {
      c.ddg.modUpdated(modId)
      c.updatedMods += modId

      parent ! PebbleMessage(self, modId, finished)
    }

    case RunTaskMessage(adjust: Adjustable[_]) => {
      sender ! adjust.run(c)
      Await.result(Future.sequence(c.pending), DURATION)
      c.pending.clear()
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId, finished: Promise[String]) => {
      val newPebble = c.ddg.parUpdated(workerRef)

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
      future onComplete {
	case Success(t) =>
	  c.updatedMods.clear()

          Await.result(Future.sequence(c.pending), DURATION)
          c.pending.clear()

          respondTo ! "done"
	case Failure(e) =>
	  e.printStackTrace()
      }
    }

    case GetDDGMessage => {
      sender ! c.ddg
    }

    case DDGToStringMessage(prefix: String) => {
      sender ! c.ddg.toString(prefix)
    }

    case CleanupWorkerMessage => {
      val futures = Set[Future[Any]]()
      for ((actorRef, parNode) <- c.ddg.pars) {
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
