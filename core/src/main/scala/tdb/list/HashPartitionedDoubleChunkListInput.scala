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

class HashPartitionedDoubleChunkListInput[T, U]
    (workers: Map[Int, ActorRef],
     partitions: Map[Int, Buffer[String]],
     conf: ListConf)
  extends Dataset[T, U] with java.io.Serializable {

  val numWorkers = workers.size

  def getWorkerRef(key: T) = {
    workers(key.hashCode().abs % workers.size)
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
    val adjustablePartitions = Map[Int, Buffer[DoubleChunkList[T, U]]]()

    for ((index, listIds) <- partitions) {
      adjustablePartitions(index) = Buffer[DoubleChunkList[T, U]]()
      for (listId <- listIds) {
        val future = workers(index) ? GetAdjustableListMessage(listId)
        adjustablePartitions(index) +=
          Await.result(future.mapTo[DoubleChunkList[T, U]], DURATION)
      }
    }

    new HashPartitionedDoubleChunkList(adjustablePartitions, conf)
  }
}
