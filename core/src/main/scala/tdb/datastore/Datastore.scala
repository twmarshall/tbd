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
import akka.pattern.{ask, pipe}
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

import tdb.Constants._
import tdb.datastore.berkeleydb.BerkeleyStore
import tdb.messages._
import tdb.Mod
import tdb.stats.Stats
import tdb.worker.WorkerConf

class Datastore(conf: WorkerConf)
    (implicit ec: ExecutionContext) {

  val store =
    conf.storeType()  match {
      case "berkeleydb" => new BerkeleyStore(conf)
      case "memory" => new MemoryStore()
    }
  store.createTable[ModId, Any]("Mods", null)

  var workerId: WorkerId = _

  private var nextModId = 0

  // Maps ModIds to sets of ActorRefs representing tasks that read them.
  private val dependencies = Map[ModId, Set[ActorRef]]()

  val inputs = Map[ModId, Any]()

  val chunks = Map[ModId, Iterable[Any]]()

  // Maps logical names of datastores to their references.
  val datastores = Map[WorkerId, ActorRef]()

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
    scala.concurrent.Await.result(getMod(mod.id, null), DURATION)
      .asInstanceOf[T]
  }

  def getMod(modId: ModId, taskRef: ActorRef): Future[_] = {
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
      val workerId = getWorkerId(modId)
      val future = datastores(workerId) ? GetModMessage(modId, taskRef)

      Stats.datastoreMisses += 1

      future
    }
  }

  def updateMod[T]
      (modId: ModId, value: T, task: ActorRef = null): Future[_] = {
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

  def addDependency(modId: ModId, taskRef: ActorRef) {
    if (dependencies.contains(modId)) {
      dependencies(modId) += taskRef
    } else {
      dependencies(modId) = Set(taskRef)
    }
  }

  def removeDependencies(modId: ModId) {
    dependencies -= modId
  }

  def clear() {
    store.clear()
    store.createTable[ModId, Any]("Mods", null)

    dependencies.clear()

    inputs.clear()
    chunks.clear()
  }
}
