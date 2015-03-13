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

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.pattern.{ask, pipe}
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import tdb.Constants._
import tdb.datastore.berkeleydb.BerkeleyStore
import tdb.messages._
import tdb.Mod
import tdb.stats.WorkerStats
import tdb.worker.WorkerInfo
import tdb.util._

class Datastore(val workerInfo: WorkerInfo, log: LoggingAdapter, id: DatastoreId)
    (implicit ec: ExecutionContext) {

  private val store =
    workerInfo.storeType  match {
      case "berkeleydb" => new BerkeleyStore(workerInfo)
      case "memory" => new MemoryStore()
    }
  store.createTable[ModId, Any]("Mods", null)

  private var nextModId = 0

  // Maps ModIds to sets of ActorRefs representing tasks that read them.
  private val dependencies = Map[ModId, Set[ActorRef]]()

  private val keyDependencies = Map[InputId, Map[Any, Set[ActorRef]]]()

  val inputs = Map[ModId, Any]()

  val chunks = Map[ModId, Iterable[Any]]()

  // Maps logical names of datastores to their references.
  val datastores = Map[WorkerId, ActorRef]()

  def getNewModId(): ModId = {
    val modId = createModId(id, workerInfo.workerId, 0, nextModId)
    nextModId += 1
    modId
  }

  def createMod[T](value: T): Mod[T] = {
    WorkerStats.modsCreated += 1
    val newModId = getNewModId()
    val future = store.put(0, newModId, value)
    scala.concurrent.Await.result(future, DURATION)

    new Mod(newModId)
  }

  def read[T](mod: Mod[T]): T = {
    scala.concurrent.Await.result(getMod(mod.id, null), DURATION)
      .asInstanceOf[T]
  }

  def readId[T](modId: ModId): T = {
    scala.concurrent.Await.result(getMod(modId, null), DURATION)
      .asInstanceOf[T]
  }

  def getMod(modId: ModId, taskRef: ActorRef): Future[_] = {
    WorkerStats.datastoreReads += 1
    if (inputs.contains(modId)) {
      store.get(1, inputs(modId))
    } else if (chunks.contains(modId)) {
      val futures = Buffer[Future[Any]]()
      for (id <- chunks(modId)) {
        futures += store.get(1, id)
      }

      Future {
        val pair =
          scala.concurrent.Await.result(Future.sequence(futures), DURATION)
        pair.toVector
      }
    } else if (store.contains(0, modId)) {
      store.get(0, modId)
    } else {
      val datastoreId = getDatastoreId(modId)
      assert(datastoreId != id)
      val future = datastores(datastoreId) ? GetModMessage(modId, taskRef)

      WorkerStats.datastoreMisses += 1

      future
    }
  }

  def updateMod[T]
      (modId: ModId, value: T, task: ActorRef = null): Future[_] = {
    WorkerStats.datastoreWrites += 1
    val futures = Buffer[Future[Any]]()

    if (!store.contains(0, modId) || store.get(0, modId) != value) {
      futures += store.put(0, modId, value)

      if (dependencies.contains(modId)) {
        for (taskRef <- dependencies(modId)) {
          if (task != taskRef) {
            futures += (taskRef ? ModUpdatedMessage(modId)).mapTo[String]
          }
        }
      }
    }

    Future.sequence(futures)
  }

  def removeMods(modIds: Iterable[ModId], task: ActorRef): Future[Any] = {
    val futures = Buffer[Future[Any]]()

    for (modId <- modIds) {
      store.delete(0, modId)

      if (dependencies.contains(modId)) {
        for (taskRef <- dependencies(modId)) {
          if (task != taskRef) {
            futures += (taskRef ? ModRemovedMessage(modId))
          }
        }

        dependencies -= modId
      }
    }

    Future.sequence(futures)
  }

  def addDependency(modId: ModId, taskRef: ActorRef) {
    if (dependencies.contains(modId)) {
      dependencies(modId) += taskRef
    } else {
      dependencies(modId) = Set(taskRef)
    }
  }

  def addKeyDependency(inputId: InputId, key: Any, taskRef: ActorRef) {
    if (!keyDependencies.contains(inputId)) {
      keyDependencies(inputId) = Map[Any, Set[ActorRef]]()
    }

    if (!keyDependencies(inputId).contains(key)) {
      keyDependencies(inputId)(key) = Set(taskRef)
    } else {
      keyDependencies(inputId)(key) += taskRef
    }
  }

  def informDependents(inputId: InputId, key: Any): Future[_] = {
    if (keyDependencies.contains(inputId) &&
        keyDependencies(inputId).contains(key)) {
      val futures = Buffer[Future[Any]]()
      for (taskRef <- keyDependencies(inputId)(key)) {
        futures += taskRef ? KeyUpdatedMessage(inputId, key)
      }
      Future.sequence(futures)
    } else {
      Future { "done" }
    }
  }

  def loadPartitions(fileName: String, range: HashRange) {
    val tableId = store.createTable[String, String](fileName, range)

    if (store.hashRange(1) != range) {
      log.warning("Loaded dataset has different hash range " +
                  store.hashRange(1) + " than provided " + range)
    }

    if (store.count(tableId) == 0) {
      log.debug("Reading " + fileName)
      store.load(1, fileName)
      log.debug("Done reading")
    } else {
      log.debug(fileName + " was already loaded.")
    }
  }

  def loadFileInfoLists
      (listIds: Buffer[String],
       newLists: Buffer[Modifier],
       respondTo: ActorRef,
       datastoreActor: ActorRef) {
    val futures = Buffer[Future[Any]]()
    var nextList = 0
    val idMap = Map[Int, (String, ActorRef)]()
    store.hashedForeach(1) {
      case (id, keys) =>
        idMap(id) = (listIds(nextList), datastoreActor)
        futures += newLists(nextList).loadInput(keys)
        nextList = (nextList + 1) % newLists.size
    }

    Future.sequence(futures).onComplete {
      case Success(v) =>
        respondTo ! new ObjHasher(idMap, store.hashRange(1).total)
      case Failure(e) => e.printStackTrace()
    }
  }

  def clear() {
    store.clear()
    store.createTable[ModId, Any]("Mods", null)

    dependencies.clear()

    inputs.clear()
    chunks.clear()
  }
}
