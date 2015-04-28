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
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.util.{Failure, Success}

import tdb.Constants._
import tdb.messages._
import tdb.Mod
import tdb.stats.WorkerStats
import tdb.worker.WorkerInfo
import tdb.util._

class Datastore(val workerInfo: WorkerInfo, log: LoggingAdapter, id: TaskId)
    (implicit ec: ExecutionContext) {

  val store = KVStore(workerInfo)
  val modTableId = store.createTable("mods", "ModId", "Any", null, false)

  val metaTableId = store.createTable("meta", "Int", "Any", null, false)

  var nextModId = 0

  // Maps ModIds to sets of ActorRefs representing tasks that read them.
  private val dependencies = Map[ModId, Set[ActorRef]]()

  val inputs = Map[ModId, Any]()

  val chunks = Map[ModId, Iterable[Any]]()

  var inputTableId = -1

  val maxModIdStep = 1000

  def getNewModId(): ModId = {
    val modId = createModId(id, 0, nextModId)
    nextModId += 1

    if (nextModId % maxModIdStep == 0) {
      val info = Await.result(
        store.get(metaTableId, id).mapTo[ModifierInfo], DURATION)
      val newInfo = new ModifierInfo(info.headId, nextModId + maxModIdStep)
      Await.result(store.put(metaTableId, id, newInfo), DURATION)
    }

    modId
  }

  def createMod[T](value: T): Mod[T] = {
    WorkerStats.modsCreated += 1
    val newModId = getNewModId()
    assert(!store.contains(modTableId, newModId))
    val future = store.put(modTableId, newModId, value)
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
      val promise = scala.concurrent.Promise[Any]
      store.get(inputTableId, inputs(modId)).onComplete {
        case Success(v) => promise.success((inputs(modId), v))
        case Failure(e) => e.printStackTrace()
      }
      promise.future
    } else if (chunks.contains(modId)) {
      val futures = Buffer[Future[Any]]()
      val keys = Map[Any, Future[Any]]()
      for (id <- chunks(modId)) {
        futures += Future {
          (id, scala.concurrent.Await.result(store.get(inputTableId, id), DURATION))
        }
      }

      Future {
        val pair =
          scala.concurrent.Await.result(Future.sequence(futures), DURATION)
        pair.toVector
      }
    } else if (store.contains(modTableId, modId)) {
      store.get(modTableId, modId)
    } else {
      println("?? " + id + " " + getDatastoreId(modId) + " " + modId + " " + this)
      ???
    }
  }

  def updateMod[T]
      (modId: ModId, value: T, task: ActorRef = null): Future[_] = {
    WorkerStats.datastoreWrites += 1
    val futures = Buffer[Future[Any]]()

    if (!store.contains(modTableId, modId) ||
        store.get(modTableId, modId) != value) {
      futures += store.put(modTableId, modId, value)

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
      store.delete(modTableId, modId)

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

  def loadPartitions(fileName: String, range: HashRange) {
    inputTableId = store.createTable(fileName, "String", "String", range, false)

    if (store.hashRange(inputTableId) != range) {
      log.warning("Loaded dataset has different hash range " +
                  store.hashRange(inputTableId) + " than provided " + range)
    }

    if (store.count(inputTableId) == 0) {
      log.debug("Reading " + fileName)
      store.load(inputTableId, fileName)
      log.debug("Done reading")
    } else {
      log.debug(fileName + " was already loaded.")
    }
  }

  def processKeys(process: Iterable[Any] => Unit) =
    store.processKeys(inputTableId, process)

  def close() {
    store.close()
  }
}
