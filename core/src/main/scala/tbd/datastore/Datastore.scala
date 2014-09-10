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
package tbd.datastore

import akka.actor.{Actor, ActorRef, ActorLogging, Props}
import akka.pattern.ask
import scala.collection.mutable.{ArrayBuffer, Map, Set}
import scala.concurrent.{Await, Future, Promise}

import tbd.{Mod}
import tbd.Constants._
import tbd.messages._

object Datastore {
  var datastoreRef: ActorRef = null

  def props(storeType: String, cacheSize: Int): Props =
    Props(classOf[Datastore], storeType, cacheSize)

  def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T]()
    mod.update(value)
    mod
  }

  def getMod(modId: ModId, workerRef: ActorRef = null): Any = {
    val future = datastoreRef ? GetModMessage(modId, workerRef)
    val ret = Await.result(future, DURATION)

    ret match {
      case NullMessage => null
      case x => x
    }
  }

  def updateMod(modId: ModId, value: Any): ArrayBuffer[Future[String]] = {
    val futuresFuture = datastoreRef ? UpdateModMessage(modId, value)
    Await.result(futuresFuture.mapTo[ArrayBuffer[Future[String]]], DURATION)
  }

  def removeMod(modId: ModId) {
    datastoreRef ! RemoveModMessage(modId)
  }

  def newModId(): Future[ModId] = {
    (datastoreRef ? CreateModMessage(null)).mapTo[ModId]
  }
}

class Datastore(storeType: String, cacheSize: Int) extends Actor with ActorLogging {
  import context.dispatcher

  val store =
    if (storeType == "memory") {
      new MemoryStore()
    } else if (storeType == "berkeleydb") {
      new BerkeleyDBStore(cacheSize, context)
    } else {
      println("WARNING: storeType '" + storeType + "' is invalid. Using " +
              "'memory' instead")
      new MemoryStore()
    }

  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Analytics
  private var startTime = System.currentTimeMillis()
  private var createCount = 0
  private var updateCount = 0
  private var getCount = 0
  private var deleteCount = 0

  private var nextModId = 0
  private def createMod(value: Any): ModId = {
    createCount += 1

    val modId = new ModId("d." + nextModId)
    nextModId += 1

    store.put(modId, value)

    modId
  }

  def getMod(modId: ModId, workerRef: ActorRef = null): Any = {
    getCount += 1

    if (workerRef != null) {
      if (dependencies.contains(modId)) {
	dependencies(modId) += workerRef
      } else {
	dependencies(modId) = Set(workerRef)
      }
    }

    store.get(modId)
  }

  def updateMod(modId: ModId, value: Any): ArrayBuffer[Future[String]] = {
    updateCount += 1
    val futures = ArrayBuffer[Future[String]]()

    if (value != getMod(modId)) {
      store.put(modId, value)

      if (dependencies.contains(modId)) {
	for (workerRef <- dependencies(modId)) {
	  val finished = Promise[String]()
	  workerRef ! ModUpdatedMessage(modId, finished)
	  futures += finished.future
	}
      }
    }

    futures
  }

  def removeMod(modId: ModId) {
    deleteCount += 1

    store.remove(modId)
  }

  def receive = {
    case CreateModMessage(value: Any) => {
      sender ! createMod(value)
    }

    case CreateModMessage(null) => {
      sender ! createMod(null)
    }

    case GetModMessage(modId: ModId, workerRef: ActorRef) => {
      val ret = getMod(modId, workerRef)

      if (ret == null) {
	sender ! NullMessage
      } else {
	sender ! ret
      }
    }

    case GetModMessage(modId: ModId, null) => {
      val ret = getMod(modId, null)

      if (ret == null) {
	sender ! NullMessage
      } else {
	sender ! ret
      }
    }

    case UpdateModMessage(modId: ModId, value: Any) => {
      sender ! updateMod(modId, value)
    }

    case UpdateModMessage(modId: ModId, null) => {
      sender ! updateMod(modId, null)
    }

    case RemoveModMessage(modId) => {
      removeMod(modId)
    }

    case CleanupMessage => {
      val elapsed = (System.currentTimeMillis() - startTime) / 1000.0
      log.info("Shutting down. reads/s = " + getCount / elapsed +
	       ", write/s = " + updateCount / elapsed + " deletes/s = " +
	       deleteCount / elapsed + ", creates/s = " + createCount / elapsed)
      store.shutdown()
      sender ! "done"
    }

    case x => {
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
