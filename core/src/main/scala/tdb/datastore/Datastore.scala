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

object Datastore {
  def props(storeType: String, cacheSize: Int): Props =
    Props(classOf[Datastore], storeType, cacheSize)
}

class Datastore(storeType: String, cacheSize: Int)
  extends Actor with ActorLogging {
  import context.dispatcher

  val store =
    storeType  match {
      case "berkeleydb" => new BerkeleyStore(cacheSize)
      case "memory" => new MemoryStore()
    }
  store.createTable[ModId, Any]("Mods", null)

  var workerId: WorkerId = _

  private var nextModId = 0

  private val lists = Map[String, Modifier]()
  private val hashedLists = Map[Int, Modifier]()

  private var nextListId = 0

  private val dependencies = Map[ModId, Set[ActorRef]]()

  val inputs = Map[ModId, Any]()

  val chunks = Map[ModId, Iterable[Any]]()

  // Maps logical names of datastores to their references.
  private val datastores = Map[WorkerId, ActorRef]()

  private var misses = 0

  def getNewModId(): ModId = {
    var newModId: Long = workerId
    newModId = newModId << 48
    newModId += nextModId

    nextModId += 1

    newModId
  }

  def createMod[T](value: T): Mod[T] = {
    val newModId = getNewModId()
    val future = store.put(0, newModId, value)
    scala.concurrent.Await.result(future, DURATION)

    new Mod(newModId)
  }

  def read[T](mod: Mod[T]): T = {
    val f = store.get(0, mod.id)
    scala.concurrent.Await.result(f, DURATION)
      .asInstanceOf[T]
  }

  private def getMod(modId: ModId, taskRef: ActorRef, respondTo: ActorRef) {
    if (inputs.contains(modId)) {
      store.get(1, inputs(modId)) pipeTo respondTo
    } else if (store.contains(0, modId)) {
      val respondTo = sender

      store.get(0, modId).onComplete {
        case Success(v) =>
          v match {
            case null => respondTo ! NullMessage
            case v => respondTo ! v
          }
        case Failure(e) =>
          e.printStackTrace()
      }
    } else if (chunks.contains(modId)) {
      val futures = Buffer[Future[Any]]()
      for (id <- chunks(modId)) {
        futures += store.get(1, id)
      }
      Future.sequence(futures).onComplete {
        case Success(arr) => respondTo ! arr.toVector
        case Failure(e) => e.printStackTrace()
      }
    } else {
      val workerId = getWorkerId(modId)
      val future = datastores(workerId) ? GetModMessage(modId, taskRef)

      Stats.datastoreMisses += 1

      future pipeTo respondTo
    }
  }

  def asyncUpdate[T](mod: Mod[T], value: T): Future[_] = {
    val futures = Buffer[Future[Any]]()

    if (!store.contains(0, mod.id) || store.get(0, mod.id) != value) {
      futures += store.put(0, mod.id, value)

      if (dependencies.contains(mod.id)) {
        for (taskRef <- dependencies(mod.id)) {
          futures += (taskRef ? ModUpdatedMessage(mod.id)).mapTo[String]
        }
      }
    }

    Future.sequence(futures)
  }

  private def updateMod
      (modId: ModId,
       value: Any,
       task: ActorRef,
       respondTo: ActorRef) {
    val futures = Buffer[Future[Any]]()

    if (!store.contains(0, modId) || store.get(0, modId) != value) {
      futures += store.put(0, modId, value)

      if (dependencies.contains(modId)) {
        for (taskRef <- dependencies(modId)) {
          if (taskRef != task) {
            futures += (taskRef ? ModUpdatedMessage(modId)).mapTo[String]
          }
        }
      }
    }

    Future.sequence(futures).onComplete {
      case value => respondTo ! "done"
    }
  }

  def receive = {
    case CreateModMessage(value: Any) =>
      sender ! createMod(value)

    case CreateModMessage(null) =>
      sender ! createMod(null)

    case GetModMessage(modId: ModId, taskRef: ActorRef) =>
      getMod(modId, taskRef, sender)

      if (dependencies.contains(modId)) {
        dependencies(modId) += taskRef
      } else {
        dependencies(modId) = Set(taskRef)
      }

    case GetModMessage(modId: ModId, null) =>
      getMod(modId, null, sender)

    case UpdateModMessage(modId: ModId, value: Any, task: ActorRef) =>
      updateMod(modId, value, task, sender)

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      updateMod(modId, value, null, sender)

    case UpdateModMessage(modId: ModId, null, task: ActorRef) =>
      updateMod(modId, null, task, sender)

    case UpdateModMessage(modId: ModId, null, null) =>
      updateMod(modId, null, null, sender)

    case RemoveModsMessage(modIds: Iterable[ModId]) =>
      for (modId <- modIds) {
        store.delete(0, modId)
        dependencies -= modId
      }

      sender ! "done"

    case CreateListIdsMessage(conf: ListConf, numPartitions: Int) =>
      val listIds = Buffer[String]()

      val newLists = Buffer[Modifier]()
      for (i <- 1 to 12) {
        val listId = nextListId + ""
        nextListId += 1
        val list =
          if (conf.chunkSize == 1)
            new DoubleListModifier(this)
          else
            new DoubleChunkListModifier(this, conf)

        lists(listId) = list
        newLists += list
        listIds += listId
      }

      if (conf.file != "") {
        val futures = Buffer[Future[Any]]()
        var nextList = 0
        store.hashedForeach(1) {
          case keys =>
            hashedLists(nextList) = newLists(nextList)
            futures += newLists(nextList).loadInput(keys)
            nextList = (nextList + 1) % newLists.size
        }

        val respondTo = sender
        Future.sequence(futures).onComplete {
          case Success(v) =>
            respondTo ! (store.hashRange(1), listIds)
          case Failure(e) => e.printStackTrace()
        }
      } else {
        sender ! (null, listIds)
      }

    case LoadPartitionsMessage
        (fileName: String,
         numWorkers: Int,
         workerIndex: Int) =>

      val range = new HashRange(workerIndex * 12, (workerIndex + 1) * 12, numWorkers * 12)

      val tableId = store.createTable[String, String](fileName, range)

      if (store.count(tableId) == 0) {
        log.debug("Reading " + fileName)
        store.load(1, fileName)
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
      datastores(workerId) = datastoreRef

    case SetIdMessage(_workerId: WorkerId) =>
      workerId = _workerId

    case ClearMessage() =>
      store.clear()
      store.createTable[ModId, Any]("Mods", null)

      lists.clear()
      hashedLists.clear()
      dependencies.clear()
      inputs.clear()
      chunks.clear()

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}

