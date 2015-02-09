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

import tdb.Constants._
import tdb.messages._
import tdb.util.ObjHasher

class HashPartitionedDoubleListInput[T, U]
    (hasher: ObjHasher[ActorRef],
     partitions: Map[ActorRef, Buffer[String]])
  extends Dataset[T, U] with java.io.Serializable {

  def getWorkerRef(key: T) = {
    hasher.getObj(key)
  }

  def put(key: T, value: U) = {
    val future = getWorkerRef(key) ? PutMessage("0", key, value)
    Await.result(future, DURATION)
  }

  def asyncPut(key: T, value: U) = {
    getWorkerRef(key) ? PutMessage("0", key, value)
  }

  def remove(key: T, value: U) = {
    getWorkerRef(key) ? RemoveMessage("0", key, value)
  }

  def load(data: Map[T, U]) = {
    for ((key, value) <- data) {
      put(key, value)
    }
  }

  def getPartitions = ???

  def getAdjustableList(): AdjustableList[T, U] = {
    val adjustablePartitions = Buffer[DoubleList[T, U]]()

    for ((datastoreRef, listIds) <- partitions) {
      for (listId <- listIds) {
        val future = datastoreRef ? GetAdjustableListMessage(listId)
        adjustablePartitions +=
          Await.result(future.mapTo[DoubleList[T, U]], DURATION)
      }
    }

    new HashPartitionedDoubleList(adjustablePartitions)
  }
}
