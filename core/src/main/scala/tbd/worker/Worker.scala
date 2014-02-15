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
  private val ddg = new DDG(log)
  private val tbd = new TBD(id, ddg, datastoreRef, self, context.system, true)

  implicit val timeout = Timeout(5 seconds)

  def receive = {
    case ModUpdatedMessage(modId) => {
      log.debug(id + " informed that mod(" + modId + ") has been updated.")
      ddg.modUpdated(modId)

      if (parent != null) {
        log.debug(id + " sending PebbleMessage to " + parent)
        val future = parent ? PebbleMessage(self)
        Await.result(future, timeout.duration)
      }

      sender ! "okay"
    }

    case RunTaskMessage(aTask: Task) => {
      task = aTask
      val output = task.func(tbd)

      if (parent == null) {
        // This is the first actor, so we're done with the initial run.
        log.debug("Contents of the DDG after initial run:\n" + ddg)
      }

      sender ! output
    }

    case PebbleMessage(workerRef: ActorRef) => {
      val newPebble = ddg.parUpdated(workerRef)
      log.debug(id + " received PebbleMessage. newPebble = " + newPebble)

      if (newPebble && parent != null) {
        log.debug(id + " sending PebbleMessage to " + parent)
        val future = parent ? PebbleMessage(self)
        Await.result(future, timeout.duration)
      }

      sender ! "okay"
    }

    case PropagateMessage => {
      log.debug(id + " actor asked to perform change propagation." + ddg.updated.size)

      while (!ddg.updated.isEmpty) {
        val node = ddg.updated.dequeue

        if (node.isInstanceOf[ReadNode[Any, Any]]) {
          val readNode = node.asInstanceOf[ReadNode[Any, Any]]
          ddg.removeSubtree(readNode)
          val future = datastoreRef ? GetMessage("mods", readNode.mod.id.value)
          val newValue = Await.result(future, timeout.duration)

          tbd.currentParent = readNode
          readNode.reader(newValue)
        } else {
          val parNode = node.asInstanceOf[ParNode]
          if (parNode.pebble1 && parNode.pebble2) {
            val future1 = parNode.workerRef1 ? PropagateMessage
            val future2 = parNode.workerRef2 ? PropagateMessage
            Await.result(future1, timeout.duration)
            Await.result(future2, timeout.duration)
          } else if (parNode.pebble1) {
            val future1 = parNode.workerRef1 ? PropagateMessage
            Await.result(future1, timeout.duration)
          } else if (parNode.pebble2) {
            val future2 = parNode.workerRef2 ? PropagateMessage
            Await.result(future2, timeout.duration)
          } else {
            log.warning("parNode in updated queue that is not pebbled.")
          }

          parNode.pebble1 = false
          parNode.pebble2 = false
        }
      }

      if (parent == null) {
        // This is the first actor, so we're done with change propagation.
        log.debug("Contents of the DDG after change propagation:\n" + ddg)
      }

      sender ! "done"
    }

    case DDGToStringMessage(prefix: String) => {
      sender ! ddg.toString(prefix)
    }

    case x => {
      log.warning(id + " received unhandled message " + x + " from " + sender)
    }
  }
}
