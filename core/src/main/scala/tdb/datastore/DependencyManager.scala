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
import akka.pattern.ask
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}

import tdb.Constants._
import tdb.messages._

class DependencyManager(implicit ec: ExecutionContext) {
  private val keyDependencies =
    mutable.Map[InputId, mutable.Map[Any, Set[ActorRef]]]()

  def addKeyDependency(inputId: InputId, key: Any, taskRef: ActorRef) {
    if (!keyDependencies.contains(inputId)) {
      keyDependencies(inputId) = mutable.Map[Any, Set[ActorRef]]()
    }

    if (!keyDependencies(inputId).contains(key)) {
      keyDependencies(inputId)(key) = Set(taskRef)
    } else {
      keyDependencies(inputId)(key) += taskRef
    }
  }

  def informDependents(inputId: InputId, key: Any): Future[_] = {
    if (keyDependencies.contains(inputId) &&
        keyDependencies(inputId).contains(key)) {
      val futures = mutable.Buffer[Future[Any]]()
      for (taskRef <- keyDependencies(inputId)(key)) {
        futures += taskRef ? KeyUpdatedMessage(inputId, key)
      }
      Future.sequence(futures)
    } else {
      Future { "done" }
    }
  }

  def clear() {
    keyDependencies.clear()
  }
}
