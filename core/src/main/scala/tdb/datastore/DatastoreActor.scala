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
import tdb.worker.WorkerConf

object DatastoreActor {
  def props(conf: WorkerConf): Props =
    Props(classOf[DatastoreActor], conf)
}

class DatastoreActor(conf: WorkerConf)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val datastore = new Datastore(conf, log)

  private val lists = Map[String, Modifier]()

  private var nextListId = 0

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

    case UpdateModMessage(modId: ModId, value: Any, task: ActorRef) =>
      datastore.updateMod(modId, value, task) pipeTo sender

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      datastore.updateMod(modId, value, null) pipeTo sender

    case UpdateModMessage(modId: ModId, null, task: ActorRef) =>
      datastore.updateMod(modId, null, task) pipeTo sender

    case UpdateModMessage(modId: ModId, null, null) =>
      datastore.updateMod(modId, null, null) pipeTo sender

    case RemoveModsMessage(modIds: Iterable[ModId]) =>
      datastore.removeMods(modIds)
      sender ! "done"

    case CreateListIdsMessage
        (conf: ListConf, workerIndex: Int, numWorkers: Int) =>
      log.debug("CreateListIdsMessage " + conf)
      val listIds = Buffer[String]()

      val partitionsPerWorker = conf.partitions / numWorkers
      val partitions =
        if (workerIndex == numWorkers - 1 &&
            conf.partitions % numWorkers != 0) {
          numWorkers % partitionsPerWorker
        } else {
          partitionsPerWorker
        }

      val newLists = Buffer[Modifier]()
      for (i <- 1 to partitions) {
        val listId = nextListId + ""
        nextListId += 1
        val list =
          if (conf.aggregate)
            new AggregatorListModifier(datastore, conf)
          else if (conf.chunkSize == 1)
            new DoubleListModifier(datastore)
          else
            new DoubleChunkListModifier(datastore, conf)

        lists(listId) = list
        newLists += list
        listIds += listId
      }

      if (conf.file != "") {
        datastore.loadFileInfoLists(listIds, newLists, sender, self)
      } else {
        val hasher = ObjHasher.makeHasher(new HashRange(
          workerIndex * partitionsPerWorker,
          (workerIndex + 1) * partitionsPerWorker,
          conf.partitions), listIds.map((_, self)))
        sender ! hasher
      }

    case LoadPartitionsMessage
        (fileName: String,
         numWorkers: Int,
         workerIndex: Int,
         partitions: Int) =>
      log.debug("LoadPartitionsMessage")
      val range = new HashRange(
        workerIndex * partitions,
        (workerIndex + 1) * partitions,
        numWorkers * partitions)

      datastore.loadPartitions(fileName, range)

      sender ! "done"

    case GetAdjustableListMessage(listId: String) =>
      sender ! lists(listId).getAdjustableList()

    case PutMessage(listId: String, key: Any, value: Any) =>
      // This solves a bug where sometimes deserialized Scala objects show up as
      // null in matches. We should figure out a better way of solving this.
      key.toString
      value.toString

      lists(listId).put(key, value) pipeTo sender

    case PutAllMessage(listId: String, values: Iterable[(Any, Any)]) =>
      val futures = Buffer[Future[Any]]()
      for ((key, value) <- values) {
        futures += lists(listId).put(key, value)
      }
      Future.sequence(futures) pipeTo sender

    case RemoveMessage(listId: String, key: Any, value: Any) =>
      lists(listId).remove(key, value) pipeTo sender

    case RemoveAllMessage(listId: String, values: Iterable[(Any, Any)]) =>
      val futures = Buffer[Future[Any]]()
      for ((key, value) <- values) {
        futures += lists(listId).remove(key, value)
      }
      Future.sequence(futures) pipeTo sender

    case RegisterDatastoreMessage(workerId: WorkerId, datastoreRef: ActorRef) =>
      datastore.datastores(workerId) = datastoreRef

    case SetIdMessage(_workerId: WorkerId) =>
      datastore.workerId = _workerId

    case ClearMessage() =>
      lists.clear()
      datastore.clear()

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}

