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
package tdb

import akka.actor.ActorRef
import akka.event.LoggingAdapter
import akka.pattern.ask
import scala.collection.mutable.{Buffer, Map, Set}
import scala.concurrent.{Await, ExecutionContext, Future}

import tdb.Constants._
import tdb.ddg._
import tdb.list._
import tdb.messages._
import tdb.worker.Task

class Context
    (val taskId: TaskId,
     val mainDatastoreId: TaskId,
     val taskRef: ActorRef,
     val masterRef: ActorRef,
     datastores: Map[TaskId, ActorRef],
     val log: LoggingAdapter)
    (implicit val ec: ExecutionContext) {

  val resolver = new Resolver(masterRef)

  val ddg = new DDG(this)

  var initialRun = true

  // Contains a list of mods that have been updated since the last run of change
  // propagation, to determine when memo matches can be made.
  val updatedMods = Set[ModId]()

  // The timestamp of the read currently being reexecuting during change
  // propagation.
  var reexecutionStart: Timestamp = _

  // The timestamp of the node immediately after the end of the read being
  // reexecuted.
  var reexecutionEnd: Timestamp = _

  var currentTime: Timestamp = ddg.ordering.base.next.base

  // The mod created by the most recent (in scope) call to mod. This is
  // what a call to write will write to.
  var currentModId: ModId = _

  // The second mod created by the most recent call to mod2, if there
  // is one.
  var currentModId2: ModId = _

  private var nextModId: Int = 0

  val pending = Buffer[Future[Any]]()

  var epoch = 0

  val buffers = Map[ListInput[Any, Any], InputBuffer[Any, Any]]()

  val bufs = Map[InputId, TraceableBuffer[Any, Any, Any]]()

  var nextNodeId = 0

  def newModId(): ModId = {
    val newModId = createModId(mainDatastoreId, taskId, nextModId)
    nextModId += 1

    newModId
  }

  def read[T](mod: Mod[T], taskRef: ActorRef = null): T = {
    readId(mod.id, taskRef).asInstanceOf[T]
  }

  def readId(modId: ModId, taskRef: ActorRef = null): Any = {
    val datastoreId = getDatastoreId(modId)
    val future = datastores(datastoreId) ? GetModMessage(modId, taskRef)
    val ret = Await.result(future, DURATION)

    ret match {
      case NullMessage => null
      case x => x
    }
  }

  def update[T](modId: ModId, value: T) {
    val message = PutMessage("mods", modId, value, taskRef)
    val datastoreId = getDatastoreId(modId)
    val future = datastores(datastoreId) ? message

    if (!initialRun) {
      pending += future

      if (ddg.reads.contains(modId)) {
        updatedMods += modId
        ddg.modUpdated(modId)
      }
    }
  }

  def remove[T](modId: ModId) {
    ddg.modRemoved(modId)
    val datastoreId = getDatastoreId(modId)
    val future = (datastores(datastoreId) ?
      RemoveModsMessage(Buffer(modId), taskRef))
    Await.result(future, DURATION)
  }

  def propagate(start: Timestamp = Timestamp.MIN_TIMESTAMP,
                end: Timestamp = Timestamp.MAX_TIMESTAMP): Future[Boolean] = {
    Future {
      var option = ddg.updated.find((timestamp: Timestamp) =>
        timestamp > start && timestamp < end)
      while (!option.isEmpty) {
        val timestamp = option.get
        val node = timestamp.node
        ddg.updated -= timestamp

        node match {
          case node: ReexecutableNode =>
            if (node.updated) {
              val oldStart = reexecutionStart
              reexecutionStart = timestamp.getNext()
              val oldEnd = reexecutionEnd
              reexecutionEnd = timestamp.end
              val oldCurrentModId = currentModId
              currentModId = node.currentModId
              val oldCurrentModId2 = currentModId2
              currentModId2 = node.currentModId2

              val oldCurrentTime = currentTime
              currentTime = timestamp

              node.updated = false

              node match {
                case getNode: GetNode =>
                  val newValue = getNode.input.get(getNode.key, taskRef)
                  getNode.getter(newValue)
                case readNode: ReadNode =>
                  val newValue = readId(readNode.modId)
                  readNode.reader(newValue)
                case readNode: Read2Node =>
                  val newValue1 = readId(readNode.modId1)
                  val newValue2 = readId(readNode.modId2)
                  readNode.reader(newValue1, newValue2)
                case readNode: Read3Node =>
                  val newValue1 = readId(readNode.modId1)
                  val newValue2 = readId(readNode.modId2)
                  val newValue3 = readId(readNode.modId3)
                  readNode.reader(newValue1, newValue2, newValue3)
                case getNode: GetFromNode =>
                  val newValue = getNode.traceable.get(getNode.parameters, -1, taskRef)
                  getNode.getter(newValue)
              }

              if (reexecutionStart < reexecutionEnd) {
                ddg.ordering.splice(reexecutionStart, reexecutionEnd, this)
              }

              reexecutionStart = oldStart
              reexecutionEnd = oldEnd
              currentModId = oldCurrentModId
              currentModId2 = oldCurrentModId2
              currentTime = oldCurrentTime
            }
          case parNode: ParNode =>
            if (parNode.updated) {
              Await.result(Future.sequence(pending), DURATION)
              pending.clear()

              val future1 = parNode.taskRef1 ? PropagateTaskMessage
              val future2 = parNode.taskRef2 ? PropagateTaskMessage

              parNode.pebble1 = false
              parNode.pebble2 = false

              Await.result(future1, DURATION)
              Await.result(future2, DURATION)
            }
          case node: Node => ???
        }

        option = ddg.updated.find((timestamp: Timestamp) =>
          timestamp > start && timestamp < end)
      }

      true
    }
  }
}
