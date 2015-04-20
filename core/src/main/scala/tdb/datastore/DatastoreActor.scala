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
package tdb.datastore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import java.io.File
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.Future
import scala.util.{Failure, Success}

import tdb.Mod
import tdb.Constants._
import tdb.datastore.berkeleydb.BerkeleyStore
import tdb.list._
import tdb.messages._
import tdb.stats.Stats
import tdb.util._
import tdb.worker.WorkerInfo

object DatastoreActor {
  def props(workerInfo: WorkerInfo, id: TaskId): Props =
    Props(classOf[DatastoreActor], workerInfo, id)
}

class DatastoreActor(workerInfo: WorkerInfo, id: TaskId)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val datastore = new Datastore(workerInfo, log, id)

  def receive = {
    case CreateModMessage(value: Any) =>
      sender ! datastore.createMod(value)

    case CreateModMessage(null) =>
      sender ! datastore.createMod(null)

    case GetModMessage(modId: ModId, taskRef: ActorRef) =>
      val respondTo = sender
      datastore.getMod(modId, taskRef).onComplete {
        case Success(v) =>
          if (v == null) {
            respondTo ! NullMessage
          } else {
            respondTo ! v
          }
        case Failure(e) => e.printStackTrace()
      }

      datastore.addDependency(modId, taskRef)

    case GetModMessage(modId: ModId, null) =>
      val respondTo = sender
      datastore.getMod(modId, null).onComplete {
        case Success(v) =>
          if (v == null) {
            respondTo ! NullMessage
          } else {
            respondTo ! v
          }
        case Failure(e) => e.printStackTrace()
      }

    case PutMessage(table, key, value, taskRef) =>
      if (table == "mods") {
        datastore.updateMod(key.asInstanceOf[ModId], value, taskRef) pipeTo sender
      } else {
        ???
      }

    case RemoveModsMessage(modIds: Iterable[ModId], taskRef: ActorRef) =>
      datastore.removeMods(modIds, taskRef) pipeTo sender

    case ClearMessage() =>
      datastore.clear()

    case FlushMessage(nodeId: NodeId, taskRef: ActorRef, initialRun: Boolean) =>
      sender ! "done"

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}

