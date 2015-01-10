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
package thomasdb.list

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.Map
import scala.concurrent.Await

import thomasdb.Constants._
import thomasdb.messages._

class DoubleListInput[T, U]
    (listId: String,
     datastoreRef: ActorRef)
  extends ListInput[T, U] with java.io.Serializable {

  def put(key: T, value: U) = {
    val future = datastoreRef ? PutMessage(listId, key, value)
    Await.result(future, DURATION)
  }

  def asyncPut(key: T, value: U) = {
    datastoreRef ? PutMessage(listId, key, value)
  }

  def update(key: T, value: U) = {
    val future = datastoreRef ? UpdateMessage(listId, key, value)
    Await.result(future, DURATION)
  }

  def remove(key: T, value: U) = {
    val future = datastoreRef ? RemoveMessage(listId, key, value)
    Await.result(future, DURATION)
  }

  def load(data: Map[T, U]) = {
    val future = datastoreRef ? LoadMessage(listId, data.asInstanceOf[Map[Any, Any]])
  }

  def putAfter(key: T, newPair: (T, U)) = {
    val future = datastoreRef ? PutAfterMessage(listId, key, newPair)
    Await.result(future, DURATION)
  }

  def getAdjustableList(): DoubleList[T, U] = {
    val future = datastoreRef ? GetAdjustableListMessage(listId)
    Await.result(future.mapTo[DoubleList[T, U]], DURATION)
  }
}
