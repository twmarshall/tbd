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

object DatastoreActor {
  def props(storeType: String, cacheSize: Int): Props =
    Props(classOf[DatastoreActor], storeType, cacheSize)
}

class DatastoreActor(storeType: String, cacheSize: Int)
  extends Actor with ActorLogging {
  import context.dispatcher

  val datastore = new Datastore(storeType, cacheSize)

  private val lists = Map[String, Modifier]()
  private val hashedLists = Map[Int, Modifier]()

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

      if (datastore.dependencies.contains(modId)) {
        datastore.dependencies(modId) += taskRef
      } else {
        datastore.dependencies(modId) = Set(taskRef)
      }

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
      //datastore.getMod(modId, null) pipeTo sender

    case UpdateModMessage(modId: ModId, value: Any, task: ActorRef) =>
      datastore.asyncUpdate(modId, value, task) pipeTo sender

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      datastore.asyncUpdate(modId, value, null) pipeTo sender

    case UpdateModMessage(modId: ModId, null, task: ActorRef) =>
      datastore.asyncUpdate(modId, null, task) pipeTo sender

    case UpdateModMessage(modId: ModId, null, null) =>
      datastore.asyncUpdate(modId, null, null) pipeTo sender

    case RemoveModsMessage(modIds: Iterable[ModId]) =>
      for (modId <- modIds) {
        datastore.store.delete(0, modId)
        datastore.dependencies -= modId
      }

      sender ! "done"

    case CreateListIdsMessage(conf: ListConf, numPartitions: Int) =>
      log.debug("CreateListIdsMessage")
      val listIds = Buffer[String]()

      val newLists = Buffer[Modifier]()
      for (i <- 1 to numPartitions) {
        val listId = nextListId + ""
        nextListId += 1
        val list =
          if (conf.chunkSize == 1)
            new DoubleListModifier(datastore)
          else
            new DoubleChunkListModifier(datastore, conf)

        lists(listId) = list
        newLists += list
        listIds += listId
      }

      if (conf.file != "") {
        val futures = Buffer[Future[Any]]()
        var nextList = 0
        datastore.store.hashedForeach(1) {
          case keys =>
            hashedLists(nextList) = newLists(nextList)
            futures += newLists(nextList).loadInput(keys)
            nextList = (nextList + 1) % newLists.size
        }

        val respondTo = sender
        Future.sequence(futures).onComplete {
          case Success(v) =>
            respondTo ! (datastore.store.hashRange(1), listIds)
          case Failure(e) => e.printStackTrace()
        }
      } else {
        sender ! (null, listIds)
      }

    case LoadPartitionsMessage
        (fileName: String,
         numWorkers: Int,
         workerIndex: Int,
         partitions: Int) =>
      log.debug("LoadPartitionsMessage")
      val range =
        new HashRange(workerIndex * partitions, (workerIndex + 1) * partitions, numWorkers * partitions)

      val tableId = datastore.store.createTable[String, String](fileName, range)

      if (datastore.store.hashRange(1) != range) {
        log.warning("Loaded dataset has different hash range " +
          datastore.store.hashRange(1) + " than provided " + range)
      }

      if (datastore.store.count(tableId) == 0) {
        log.debug("Reading " + fileName)
        datastore.store.load(1, fileName)
        log.debug("Done reading")
      } else {
        log.debug(fileName + " was already loaded.")
      }

      sender ! "done"

    case GetAdjustableListMessage(listId: String) =>
      sender ! lists(listId).getAdjustableList()

    case PutMessage(listId: String, key: Any, value: Any) =>
      // This solves a bug where sometimes deserialized Scala objects show up as
      // null in matches. We should figure out a better way of solving this.
      key.toString
      value.toString

      if (hashedLists.size == 0) {
        lists(listId).put(key, value) pipeTo sender
      } else {
        val hash = key.hashCode().abs % hashedLists.size
        hashedLists(hash).put(key, value) pipeTo sender
      }

    case RemoveMessage(listId: String, key: Any, value: Any) =>
      lists(listId).remove(key, value) pipeTo sender

    case RegisterDatastoreMessage(workerId: WorkerId, datastoreRef: ActorRef) =>
      datastore.datastores(workerId) = datastoreRef

    case SetIdMessage(_workerId: WorkerId) =>
      datastore.workerId = _workerId

    case ClearMessage() =>
      lists.clear()
      hashedLists.clear()
      datastore.clear()

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}

