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

import akka.event.Logging
import scala.collection.mutable.{Buffer, Set, Map}
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.ddg.{Node, Timestamp}
import tbd.master.Main
import tbd.worker.Worker

class Context(val id: String, val worker: Worker) {
  import worker.context.dispatcher

  var initialRun = true

  // The Node representing the currently executing reader.
  var currentParent: Node = worker.ddg.root

  val log = Logging(worker.context.system, "TBD" + id)

  // Contains a list of mods that have been updated since the last run of change
  // propagation, to determine when memo matches can be made.
  val updatedMods = Set[ModId]()

  // The timestamp of the read currently being reexecuting during change
  // propagation.
  var reexecutionStart: Timestamp = _

  // The timestamp of the node immediately after the end of the read being
  // reexecuted.
  var reexecutionEnd: Timestamp = _

  // The mod created by the most recent (in scope) call to mod. This is
  // what a call to write will write to.
  var currentMod: Mod[Any] = _

  // The second mod created by the most recent call to mod2, if there
  // is one.
  var currentMod2: Mod[Any] = _

  // A unique id to assign to workers forked from this context.
  var workerId = 0

  private var nextModId = 0

  val pending = Buffer[Future[String]]()

  def newModId(): ModId = {
    nextModId += 1
    id + "." + nextModId
  }
}
