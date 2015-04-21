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
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

import tdb.Constants._
import tdb.list._
import tdb.messages._
import tdb.worker.WorkerInfo
import tdb.util.HashRange

object ModifierActor {
  def props
      (conf: ListConf,
       workerInfo: WorkerInfo,
       datastoreId: TaskId,
       range: HashRange,
       masterRef: ActorRef): Props =
    Props(
      classOf[ModifierActor], conf, workerInfo, datastoreId, range, masterRef)
}

class ModifierActor
    (conf: ListConf,
     workerInfo: WorkerInfo,
     datastoreId: TaskId,
     range: HashRange,
     masterRef: ActorRef)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val datastore = new Datastore(workerInfo, log, datastoreId)

  val modifier =
    if (conf.chunkSize == 1)
      new DoubleListModifier(datastore)
    else
      new DoubleChunkListModifier(datastore, conf)

  def receive = {
    case CreateModMessage(value) =>
      sender ! datastore.createMod(value)

    case GetModMessage(modId: ModId, taskRef) =>
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

    case RemoveModsMessage(modIds: Iterable[ModId], taskRef: ActorRef) =>
      datastore.removeMods(modIds, taskRef) pipeTo sender

    case LoadFileMessage(fileName: String, recovery) =>
      datastore.loadPartitions(fileName, range)
      datastore.processKeys {
        case keys => modifier.loadInput(keys)
      }

      if (recovery) {
        sender ! "done"
      } else {
        (masterRef ? FileLoadedMessage(datastoreId, fileName)) pipeTo sender
      }

    case GetAdjustableListMessage() =>
      sender ! modifier.getAdjustableList()

    case ToBufferMessage() =>
      sender ! modifier.toBuffer()

    case PutMessage(table: String, key: Any, value: Any, taskRef) =>
      val futures = mutable.Buffer[Future[Any]]()
      futures += modifier.put(key, value)
      Future.sequence(futures) pipeTo sender

    case PutAllMessage(values: Iterable[(Any, Any)]) =>
      val futures = mutable.Buffer[Future[Any]]()
      for ((key, value) <- values) {
        futures += modifier.put(key, value)
      }
      Future.sequence(futures) pipeTo sender

    case RemoveMessage(key: Any, value: Any) =>
      val futures = mutable.Buffer[Future[Any]]()
      futures += modifier.remove(key, value)
      Future.sequence(futures) pipeTo sender

    case RemoveAllMessage(values: Iterable[(Any, Any)]) =>
      val futures = mutable.Buffer[Future[Any]]()
      for ((key, value) <- values) {
        futures += modifier.remove(key, value)
      }
      Future.sequence(futures) pipeTo sender

    case FlushMessage(nodeId: NodeId, taskRef: ActorRef, initialRun: Boolean) =>
      sender ! "done"

    case x =>
      log.warning("ModifierActor received unhandled message " + x +
        " from " + sender)
  }

  override def postStop() {
    datastore.close()
  }
}
