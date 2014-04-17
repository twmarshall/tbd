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
import scala.collection.mutable.{Map, Set}
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import tbd.TBD
import tbd.datastore.Dataset
import tbd.ddg.{DDG, ParNode, ReadNode}
import tbd.memo.MemoEntry
import tbd.messages._
import tbd.mod.ModId

object Worker {
  def props(id: String, datastoreRef: ActorRef, parent: ActorRef): Props =
    Props(classOf[Worker], id, datastoreRef, parent)
}

class Worker(id: String, aDatastoreRef: ActorRef, parent: ActorRef)
  extends Actor with ActorLogging {
  log.info("Worker " + id + " launched")

  val datastoreRef = aDatastoreRef
  val ddg = new DDG(log, id, this)
  val memoTable = Map[List[Any], MemoEntry]()
  val datasets = Set[Dataset[Any]]()

  private var task: Task = null
  private val tbd = new TBD(id, this)

  implicit val timeout = Timeout(30 seconds)

  // During change propagation, represents the number of child workers this
  // worker is waiting to receive FinishedPropagatingMessages from before it
  // can continue.
  var awaiting = 0

  def propagate(): Boolean = {
    while (!ddg.updated.isEmpty) {
      val node = ddg.updated.dequeue

      if (node.isInstanceOf[ReadNode]) {
        val readNode = node.asInstanceOf[ReadNode]
        log.debug("Reexecuting read of " + readNode.mod.id)

        val memoNodes = ddg.removeSubtree(readNode, true)

        val newValue =
          if (tbd.mods.contains(readNode.mod.id)) {
            tbd.mods(readNode.mod.id)
          } else {
            readNode.mod.read()
          }

        tbd.currentParent = readNode
        readNode.updated = false
        readNode.reader(newValue)

        for (node <- memoNodes) {
          if (node.parent == null) {
            ddg.removeSubtree(node, false)
          }
        }
      } else {
        val parNode = node.asInstanceOf[ParNode]
        //assert(awaiting == 0)

        if (parNode.pebble1) {
          parNode.workerRef1 ! PropagateMessage
          parNode.pebble1 = false
          awaiting = 1
        }

        if (parNode.pebble2) {
          parNode.workerRef2 ! PropagateMessage
          parNode.pebble2 = false
          awaiting += 1
        }
        //assert(awaiting > 0)

        return false
      }
    }

    return true
  }

  private def get(key: ModId): Any = {
    val ret = tbd.mods(key)

    if (ret == null) {
      NullMessage
    } else {
      ret
    }
  }

  def receive = {
    case GetMessage(table: String, key: ModId) => {
      if (tbd.mods(key) == null) {
        sender ! NullMessage
      } else {
        sender ! tbd.mods(key)
      }

      assert(table == "mods")
    }

    case ReadModMessage(modId: ModId, workerRef: ActorRef) => {
      sender ! get(modId)

      if (tbd.dependencies.contains(modId)) {
        tbd.dependencies(modId) += workerRef
      } else {
        tbd.dependencies(modId) = Set(workerRef)
      }
    }

    case ModUpdatedMessage(modId: ModId, respondTo: ActorRef) => {
      parent ! PebbleMessage(self, modId, respondTo)

      ddg.modUpdated(modId)
      tbd.updatedMods += modId
    }

    case RunTaskMessage(aTask: Task) => {
      task = aTask
      val output = task.func(tbd)

      sender ! output
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId, respondTo: ActorRef) => {
      val newPebble = ddg.parUpdated(workerRef)

      if (newPebble) {
        parent ! PebbleMessage(self, modId, respondTo)
      } else {
        respondTo ! PebblingFinishedMessage(modId)
      }
    }

    case PebblingFinishedMessage(modId: ModId) => {
      assert(tbd.awaiting > 0)
      tbd.awaiting -= 1

      if (!tbd.initialRun && tbd.awaiting == 0 && awaiting == 0) {
        parent ! FinishedPropagatingMessage
      }
    }

    case PropagateMessage => {
      tbd.initialRun = false
      val done = propagate()

      if (done && tbd.awaiting == 0) {
        parent ! FinishedPropagatingMessage
      }
    }

    case FinishedPropagatingMessage => {
      assert(awaiting > 0)
      awaiting -= 1

      if (awaiting == 0) {
        val done = propagate()

        if (done && tbd.awaiting == 0) {
          parent ! FinishedPropagatingMessage
        }
      }
    }

    case GetDDGMessage => {
      sender ! ddg
    }

    case DDGToStringMessage(prefix: String) => {
      sender ! ddg.toString(prefix)
    }

    case CleanupWorkerMessage => {
      val datastoreFuture = datastoreRef ? CleanUpMessage(self, datasets)

      val futures = Set[Future[Any]]()
      for ((actorRef, parNode) <- ddg.pars) {
        futures += actorRef ? CleanupWorkerMessage
        actorRef ! akka.actor.PoisonPill
      }

      for (future <- futures) {
        Await.result(future, timeout.duration)
      }

      Await.result(datastoreFuture, timeout.duration)

      sender ! "done"
    }

    case x => {
      log.warning(id + " received unhandled message " + x + " from " + sender)
    }
  }
}
