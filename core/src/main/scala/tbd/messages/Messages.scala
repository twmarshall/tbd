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

import tbd.{Adjustable, Changeable, TBD}
import tbd.ddg.Node
import tbd.mod.{Mod, ModId}
import tbd.worker.Task

// DDG
case class AddReadMessage(modId: ModId, parent: Node)
case class AddWriteMessage(modId: ModId, parent: Node)
case class ToStringMessage

// Datastore
case class CreateTableMessage(table: String)
case class GetMessage(table: String, key: Any)
case class PutMessage(table: String, key: Any, value: Any)
case class UpdateMessage(table: String, key: Any, value: Any)

case class CreateModMessage(value: Any)
case class UpdateModMessage(modId: ModId, value: Any)
case class ReadModMessage(modId: ModId, workerRef: ActorRef)

case class PutMatrixMessage(table: String, key: Any, value: Array[Array[Int]])
case class GetArrayMessage(table: String)
case class GetListMessage(table: String)
case class GetUpdatedMessage
case class NullMessage

// Master
case class RunMessage(adjust: Adjustable)
case class ShutdownMessage

// Worker
case class ModUpdatedMessage(modId: ModId)
case class PebbleMessage(workerRef: ActorRef)
case class PropagateMessage
case class RunTaskMessage(func: Task)
case class DDGToStringMessage(prefix: String)
