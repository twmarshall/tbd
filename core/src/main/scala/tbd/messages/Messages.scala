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
package tbd.messages

import akka.actor.ActorRef
import scala.collection.mutable.Set
import scala.language.existentials

import tbd.{Adjustable, Changeable, TBD}
import tbd.Constants._
import tbd.ddg.Node

// Datastore
case class GetModMessage(modId: ModId, task: ActorRef)
case class UpdateModMessage(modId: ModId, value: Any, task: ActorRef)
case class RemoveModsMessage(mods: Iterable[ModId])
case class NullMessage()

case class DBPutMessage(key: ModId, value: Any)
case class DBGetMessage(key: ModId)
case class DBDeleteMessage(key: ModId)
case class DBContainsMessage(key: ModId)
case class DBShutdownMessage()

// Master
case class RegisterMutatorMessage()
case class RunMutatorMessage(adjust: Adjustable[_], mutatorId: Int)
case class PropagateMutatorMessage(mutatorId: Int)
case class GetMutatorDDGMessage(mutatorId: Int)
case class ScheduleTaskMessage(id: String, parent: ActorRef)
case class ShutdownMutatorMessage(mutatorId: Int)

// Worker
case class RegisterWorkerMessage(worker: ActorRef)
case class GetDatastoreMessage()

// Task
case class ModUpdatedMessage(modId: ModId)
case class PebbleMessage(taskRef: ActorRef, modId: ModId)
case class PropagateTaskMessage()
case class RunTaskMessage(adjust: Adjustable[_])
case class GetTaskDDGMessage()
case class ShutdownTaskMessage()
