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

object DependencyManager {
  var managerRef: ActorRef = null

  def props(storeType: String, cacheSize: Int): Props =
    Props(classOf[DependencyManager], storeType, cacheSize)

  def addDependency(modId: ModId, workerRef: ActorRef) {
    managerRef ! AddDependencyMessage(modId, workerRef)
  }

  def modUpdated(modId: ModId, worker: ActorRef): Future[String] = {
    (managerRef ? UpdatedModMessage(modId, worker)).mapTo[String]
  }
}

class DependencyManager(storeType: String, cacheSize: Int) extends Actor with ActorLogging {
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

  def modUpdated(modId: ModId, worker: ActorRef, respondTo: ActorRef) {
    val futures = ArrayBuffer[Future[String]]()

    if (dependencies.contains(modId)) {
      for (workerRef <- dependencies(modId)) {
        if (workerRef != worker) {
	  val finished = Promise[String]()
	  workerRef ! ModUpdatedMessage(modId, finished)
	  futures += finished.future
        }
      }
    }

    Future.sequence(futures).onComplete {
      case value => respondTo ! "done"
    }
  }

  def receive = {
    case AddDependencyMessage(modId: ModId, workerRef: ActorRef) => {
      if (dependencies.contains(modId)) {
	dependencies(modId) += workerRef
      } else {
	dependencies(modId) = Set(workerRef)
      }
    }

    case UpdatedModMessage(modId: ModId, worker: ActorRef) => {
      modUpdated(modId, worker, sender)
    }

    case UpdatedModMessage(modId: ModId, null) => {
      modUpdated(modId, null, sender)
    }

    case CleanupMessage => {
      store.shutdown()
      sender ! "done"
    }

    case x => {
      log.warning("DependencyManager actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
