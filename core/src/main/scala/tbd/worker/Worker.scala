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
import akka.util.Timeout
import scala.collection.mutable.Set
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.TBD
import tbd.ddg.{DDG, ParNode, ReadNode}
import tbd.messages._
import tbd.mod.ModId

object Worker {
  def props[T](id: String, datastoreRef: ActorRef, parent: ActorRef): Props =
    Props(classOf[Worker[T]], id, datastoreRef, parent)
}

class Worker[T](id: String, datastoreRef: ActorRef, parent: ActorRef)
  extends Actor with ActorLogging {
  log.info("Worker " + id + " launched")
  private var task: Task = null
  private val ddg = new DDG(log, id)
  private val tbd = new TBD(id, ddg, datastoreRef, self, context.system, true)

  implicit val timeout = Timeout(5 seconds)

  // During change propagation, represents the number of child workers this
  // worker is waiting to receive FinishedPropagatingMessages from before it
  // can continue.
  var awaiting = 0

  var propagating = false

  def propagate(): Boolean = {
    while (!ddg.updated.isEmpty) {
      val node = ddg.updated.dequeue

      if (node.isInstanceOf[ReadNode[Any, Any]]) {
        val readNode = node.asInstanceOf[ReadNode[Any, Any]]
        log.debug("Propagating read of mod(" + readNode.mod.id + ")")
        ddg.removeSubtree(readNode)

        val future = datastoreRef ? GetMessage("mods", readNode.mod.id.value)
        val newValue = Await.result(future, timeout.duration)

        tbd.currentParent = readNode
        readNode.updated = false
        readNode.reader(newValue)
      } else {
        val parNode = node.asInstanceOf[ParNode]
        log.debug("Updating ParNode.")
        if (parNode.pebble1 && parNode.pebble2) {
          val future1 = parNode.workerRef1 ! PropagateMessage
          val future2 = parNode.workerRef2 ! PropagateMessage

          parNode.pebble1 = false
          parNode.pebble2 = false

          awaiting = 2
          return false
        } else if (parNode.pebble1) {
          val future1 = parNode.workerRef1 ! PropagateMessage

          parNode.pebble1 = false

          awaiting = 1
          return false
        } else if (parNode.pebble2) {
          val future2 = parNode.workerRef2 ! PropagateMessage

          parNode.pebble2 = false

          awaiting = 1
          return false
        } else {
          log.warning("parNode in updated queue that is not pebbled.")
        }

      }
    }

    return true
  }

  def receive = {
    case ModUpdatedMessage(modId) => {
      log.debug("Informed that mod(" + modId + ") has been updated.")
      ddg.modUpdated(modId)

      log.debug("Sending PebbleMessage to " + parent)
      parent ! PebbleMessage(self, modId)
    }

    case RunTaskMessage(aTask: Task) => {
      task = aTask
      val output = task.func(tbd)

      sender ! output
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId) => {
      log.debug("Received PebbleMessage for " + modId)
      val newPebble = ddg.parUpdated(workerRef)

      if (newPebble) {
        log.debug("Sending PebbleMessage to " + parent)
        val future = parent ! PebbleMessage(self, modId)
      } else {
        log.debug("Sending PebblingFinishedMessage(" + modId + ") to datastore.")
        datastoreRef ! PebblingFinishedMessage(modId)
      }
    }

    case PebblingFinishedMessage(modId: ModId) => {
      assert(tbd.awaiting > 0)
      tbd.awaiting -= 1
      log.debug("Received PebblingFinishedMessage. Still waiting on " + tbd.awaiting + " with updated=" + ddg.updated.size)

      if (propagating & tbd.awaiting == 0 && awaiting == 0) {
        log.debug("Sending FinishedPropagatingMessage.")
        parent ! FinishedPropagatingMessage
      }
    }

    case PropagateMessage => {
      log.debug("Asked to perform change propagation. updated: " + ddg.updated.size)

      propagating = true
      val done = propagate()

      if (done && tbd.awaiting == 0) {
        log.debug("Sending FinishedPropagatingMessage to " + parent)
        parent ! FinishedPropagatingMessage
      }
    }

    case FinishedPropagatingMessage => {
      assert(awaiting > 0)
      awaiting -= 1
      log.debug("Received FinishedPropagatingMessage. Still waiting on " + awaiting)

      if (awaiting == 0) {
        val done = propagate()

        if (propagating && done && tbd.awaiting == 0) {
          log.debug("Sending FinishedPropagatingMessage to " + parent)
          parent ! FinishedPropagatingMessage
        }
      }
    }

    case DDGToStringMessage(prefix: String) => {
      sender ! ddg.toString(prefix)
    }

    case x => {
      log.warning(id + " received unhandled message " + x + " from " + sender)
    }
  }
}
