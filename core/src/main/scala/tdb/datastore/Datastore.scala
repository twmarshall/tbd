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
import tdb.list._
import tdb.messages._
import tdb.stats.Stats
import tdb.util._

object Datastore {
  def props(storeType: String, cacheSize: Int): Props = {
    storeType match {
      case "berkeleydb" =>
        Props(classOf[BerkeleyDBStore], cacheSize)
      case "memory" =>
        Props(classOf[MemoryStore])
    }
  }
}

trait Datastore extends Actor with ActorLogging {
  import context.dispatcher

  def put(key: ModId, value: Any)

  def asyncPut(key: ModId, value: Any): Future[Any]

  def get(key: ModId): Any

  def asyncGet(key: ModId): Future[Any]

  def remove(key: ModId)

  def contains(key: ModId): Boolean

  def clear()

  def shutdown()

  var workerId: WorkerId = _

  private var nextModId = 0

  private val lists = Map[String, Modifier]()
  private val hashedLists = Map[Int, Modifier]()

  private var nextListId = 0

  private val dependencies = Map[ModId, Set[ActorRef]]()

  val inputs = Map[ModId, String]()

  val chunks = Map[ModId, Iterable[String]]()

  // Maps logical names of datastores to their references.
  private val datastores = Map[WorkerId, ActorRef]()

  private var misses = 0

  var database = new BerkeleyDatabase()

  private var inputStore: BerkeleyInputStore = null

  def getNewModId(): ModId = {
    var newModId: Long = workerId
    newModId = newModId << 48
    newModId += nextModId

    nextModId += 1

    newModId
  }

  def createMod[T](value: T): Mod[T] = {
    val newModId = getNewModId()
    put(newModId, value)

    new Mod(newModId)
  }

  def read[T](mod: Mod[T]): T = {
    get(mod.id).asInstanceOf[T]
  }

  private def getMod(modId: ModId, taskRef: ActorRef, respondTo: ActorRef) {
    if (inputs.contains(modId)) {
      inputStore.get(inputs(modId)) pipeTo respondTo
    } else if (contains(modId)) {
      asyncGet(modId) pipeTo sender
    } else if (chunks.contains(modId)) {
      val futures = Buffer[Future[Any]]()
      for (id <- chunks(modId)) {
        futures += inputStore.get(id)
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
    val futures = Buffer[Future[String]]()

    if (!contains(mod.id) || get(mod.id) != value) {
      put(mod.id, value)

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

    if (!contains(modId) || get(modId) != value) {
      futures += asyncPut(modId, value)

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
        remove(modId)
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
        val process2 = (keys: Iterable[String]) => {
          hashedLists(nextList) = newLists(nextList)
          futures += newLists(nextList).loadInput(keys)
          nextList = (nextList + 1) % newLists.size
        }

        inputStore.iterateInput(process2, numPartitions)

        val respondTo = sender
        Future.sequence(futures).onComplete {
          case Success(v) =>
            respondTo ! (inputStore.hashRange, listIds)
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
      inputStore = database.createInputStore(fileName, range)

      if (inputStore.count() == 0) {
        val file = new File(fileName)
        val fileSize = file.length()
        val partitionSize = fileSize / numWorkers

        val process = (key: String, value: String) => {
          if (range.fallsInside(key)) {
            inputStore.put(key, value)
          }
        }

        log.debug("Reading " + fileName + " from " + 0 + ", size " +
                  fileSize)

        FileUtil.readKeyValueFile(
          fileName, fileSize, 0, fileSize, process)

        log.debug("Done reading")
      } else {
        log.debug(fileName + " was already loaded.")
      }

      sender ! "done"

    case GetAdjustableListMessage(listId: String) =>
      sender ! lists(listId).getAdjustableList()

    case LoadMessage(listId: String, data: Map[Any, Any]) =>
      lists(listId).load(data)
      sender ! "okay"

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
      if (inputStore != null) {
        inputStore.close()
      }
      clear()
      lists.clear()

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}

