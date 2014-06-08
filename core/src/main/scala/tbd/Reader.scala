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
import akka.pattern.ask
import scala.concurrent.Await

import tbd.Constants._
import tbd.messages._
import tbd.mod.{Mod, AdjustableChunkList, AdjustableList}
import tbd.worker.Worker

class Reader(worker: Worker) {
  def getMod[T](key: Int): Mod[T] = {
    val modFuture = worker.datastoreRef ? GetModMessage("input", key)
    Await.result(modFuture, DURATION)
      .asInstanceOf[Mod[T]]
  }

  def getAdjustableList[T, V](
      partitions: Int = 8,
      valueMod: Boolean = true): AdjustableList[T, V] = {
    val message = GetAdjustableListMessage("input", partitions, valueMod)

    val adjustableListFuture = worker.datastoreRef ? message

    val adjustableList = Await.result(adjustableListFuture, DURATION)
    worker.adjustableLists += adjustableList.asInstanceOf[AdjustableList[Any, Any]]
    adjustableList.asInstanceOf[AdjustableList[T, V]]
  }

  def getChunkList[T, V](
      partitions: Int = 8,
      chunkSize: Int = 0,
      chunkSizer: V => Int = (v: V) => 0,
      valueMod: Boolean = true): AdjustableChunkList[T, V] = {
    val anySizer = chunkSizer.asInstanceOf[Any => Int]
    val message = GetChunkListMessage("input", partitions, chunkSize, anySizer, valueMod)

    val chunkListFuture = worker.datastoreRef ? message

    val chunkList = Await.result(chunkListFuture, DURATION)
    worker.adjustableLists += chunkList.asInstanceOf[AdjustableChunkList[Any, Any]]
    chunkList.asInstanceOf[AdjustableChunkList[T, V]]
  }
}
