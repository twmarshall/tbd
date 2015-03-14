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
package tdb.list

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.Await

import tdb.{Input, Mutator}
import tdb.Constants._
import tdb.messages._

trait ListInput[T, U] extends Input[T, U] {

  def inputId: InputId

  // Inserts all of the elements from data into this ListInput. Assumes that
  // the list is currently empty.
  def load(data: Map[T, U])

  def getAdjustableList(): AdjustableList[T, U]

  def getBuffer(): InputBuffer[T, U]

  def flush(): Unit
}
