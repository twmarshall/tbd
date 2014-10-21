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
package tbd

import akka.actor.ActorRef
import akka.event.Logging
import akka.pattern.ask
import scala.collection.mutable.{Buffer, Set}
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.ddg.{DDG, Node, Timestamp}
import tbd.messages._
import tbd.worker.Task

class Context(val id: String, val task: Task, val datastore: ActorRef) {
  import task.context.dispatcher

  val log = Logging(task.context.system, "TBD" + id)

  val ddg = new DDG(id)

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
  var currentMod: Mod[Any] = _

  // The second mod created by the most recent call to mod2, if there
  // is one.
  var currentMod2: Mod[Any] = _

  // A unique id to assign to tasks forked from this context.
  var taskId = 0

  private var nextModId = 0

  val pending = Buffer[Future[String]]()

  def newModId(): ModId = {
    nextModId += 1
    id + "." + nextModId
  }

  def read[T](mod: Mod[T], taskRef: ActorRef = null): T = {
    val future = datastore ? GetModMessage(mod.id, taskRef)
    val ret = Await.result(future, DURATION)

    ret match {
      case NullMessage => null.asInstanceOf[T]
      case x: T => x
    }
  }

  def update[T](mod: Mod[T], value: T) {
    val message = UpdateModMessage(mod.id, value, task.self)
    val future = (datastore ? message).mapTo[String]

    if (!initialRun) {
      pending += future

      if (ddg.reads.contains(mod.id)) {
	updatedMods += mod.id
	ddg.modUpdated(mod.id)
      }
    }
  }
}
