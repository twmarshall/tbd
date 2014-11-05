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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}

import tbd.Mod
import tbd.Constants._
import tbd.messages._

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  import context.dispatcher

  private var workerId: String = _

  private val mods = Map[ModId, Any]()

  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps logical names of datastores to their references.
  private val datastores = Map[String, ActorRef]()

  private var nextModId = 0

  def createMod(value: Any): Mod[Any] = {
    val modId = workerId + ":" + nextModId
    nextModId += 1

    mods(modId) = value

    new Mod(modId)
  }

  def getMod(modId: ModId, taskRef: ActorRef): Any = {
    if (mods.contains(modId)) {
      if (mods(modId) == null)
	NullMessage
      else
	mods(modId)
    } else {
      val workerId = modId.split(":")(0)
      val future = datastores(workerId) ? GetModMessage(modId, taskRef)
      Await.result(future, DURATION)
    }
  }

  def updateMod
      (modId: ModId,
       value: Any,
       task: ActorRef,
       respondTo: ActorRef) {
    val futures = Buffer[Future[String]]()

    if (!mods.contains(modId) || mods(modId) != value) {
      mods(modId) = value

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
      sender ! getMod(modId, taskRef)

      if (dependencies.contains(modId)) {
	dependencies(modId) += taskRef
      } else {
	dependencies(modId) = Set(taskRef)
      }

    case GetModMessage(modId: ModId, null) =>
      sender ! getMod(modId, null)

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
	mods -= modId
	dependencies -= modId
      }

      sender ! "done"

    case RegisterDatastoreMessage(workerId: String, datastoreRef: ActorRef) =>
      datastores(workerId) = datastoreRef

    case SetIdMessage(_workerId: String) =>
      workerId = _workerId

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}
