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
package tdb.messages

import akka.actor.ActorRef
import java.io.BufferedWriter
import scala.collection.mutable.{Map, Set}
import scala.language.existentials

import tdb.{Adjustable, Changeable, TDB}
import tdb.Constants._
import tdb.ddg.Node
import tdb.list.ListConf
import tdb.util.HashRange
import tdb.worker.WorkerInfo

// Datastore
case class CreateModMessage(value: Any)
case class GetModMessage(modId: ModId, task: ActorRef)
case class UpdateModMessage(modId: ModId, value: Any, task: ActorRef)
case class RemoveModsMessage(mods: Iterable[ModId], taskRef: ActorRef)
case class NullMessage()
case class ClearMessage()
case class LoadFileMessage(fileName: String, recovery: Boolean = false)
case class FlushMessage(nodeId: NodeId, taskId: TaskId, taskRef: ActorRef, initialRun: Boolean)

// Master
case class RegisterMutatorMessage()
case class RunMutatorMessage(adjust: Adjustable[_], mutatorId: Int)
case class PropagateMutatorMessage(mutatorId: Int)
case class GetMutatorDDGMessage(mutatorId: Int)
case class PrintMutatorDDGDotsMessage(mutatorId: Int, nextName: Int, output: BufferedWriter)
case class MutatorToBufferMessage(datastoreId: TaskId)
case class ScheduleTaskMessage(
  name: String, parentId: TaskId, datastoreId: TaskId, adjust: Adjustable[_])
case class ShutdownMutatorMessage(mutatorId: Int)
case class ResolveMessage(datastoreId: TaskId)

case class CreateListMessage(conf: ListConf)
case class FileLoadedMessage(datastoreId: TaskId, fileName: String)
case class GetAdjustableListMessage()
case class ToBufferMessage()
case class PutMessage(table: String, key: Any, value: Any, taskRef: ActorRef)
case class PutInMessage(column: String, key: Any, value: Any)
case class PutAllMessage(values: Iterable[(Any, Any)])
case class PutAllInMessage(column: String, values: Iterable[(Any, Any)])
case class GetMessage(key: Any, taskRef: ActorRef)
case class RemoveMessage(key: Any, value: Any)
case class RemoveAllMessage(values: Iterable[(Any, Any)])

// Worker
case class RegisterWorkerMessage(workerInfo: WorkerInfo)
case class CreateTaskMessage(taskId: TaskId, parentId: TaskId)
case class GetDatastoreMessage()
case class SplitFileMessage(dir: String, fileName: String, partitions: Int)
case class CreateDatastoreMessage(
  listConf: ListConf, datastoreId: TaskId, thisRange: HashRange)

// Task
case class ModUpdatedMessage(modId: ModId)
case class NodeUpdatedMessage(nodeId: NodeId)
case class ModRemovedMessage(modId: ModId)
case class KeyUpdatedMessage(inputId: InputId, key: Any)
case class KeyRemovedMessage(inputId: InputId, key: Any)
case class PebbleMessage(taskRef: ActorRef, modId: ModId)
case class PropagateTaskMessage()
case class RunTaskMessage(adjust: Adjustable[_])
case class GetTaskDDGMessage()
case class PrintDDGDotsMessage(nextName: Int, output: BufferedWriter)

case class GetFromMessage(parameters: (Any, Iterable[String]), nodeId: NodeId, taskRef: ActorRef)
