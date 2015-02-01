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
import scala.collection.mutable.Map
import scala.concurrent.Future

import tdb.Constants.ModId
import tdb.messages.NullMessage

class MemoryStore extends Datastore {
  import context.dispatcher

  private val values = Map[ModId, Any]()
  private val input = Map[String, String]()

  def put(key: ModId, value: Any) {
    values(key) = value
  }

  def asyncPut(key: ModId, value: Any): Future[Any] = {
    values(key) = value
    Future { "done" }
  }

  def get(key: ModId): Any = {
    values(key)
  }

  def asyncGet(key: ModId): Future[Any] = {
    Future {
      val value = values(key)
      if (value == null) {
        NullMessage
      } else {
        values(key)
      }
    }
  }

  def remove(key: ModId) {
    values -= key
  }

  def contains(key: ModId) = {
    values.contains(key)
  }

  def clear() {
    values.clear()
  }

  def shutdown() {
    values.clear()
  }

  def putInput(key: String, value: String) {
    input(key) = value
  }

  def retrieveInput(inputName: String): Boolean = false

  def iterateInput(process: String => Unit) {
    for (key <- input.keys) {
      process(key)
    }
  }

  def getInput(key: String) = {
    Future {
      (key, input(key))
    }
  }
}
