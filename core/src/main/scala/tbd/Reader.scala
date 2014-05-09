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
import tbd.mod.{Mod, AdjustableList}
import tbd.worker.Worker

class Reader(worker: Worker) {
  def get[T](key: Int): T = {
    val modFuture = worker.datastoreRef ? GetMessage("input", key)
    Await.result(modFuture, DURATION)
      .asInstanceOf[T]
  }

  def getArray[T](): Array[T] = {
    val arrayFuture = worker.datastoreRef ? GetArrayMessage("input")
    Await.result(arrayFuture, DURATION)
      .asInstanceOf[Array[T]]
  }

  def getAdjustableList[T, V](partitions: Int = 8): AdjustableList[T, V] = {
    val adjustableListFuture =
      worker.datastoreRef ? GetAdjustableListMessage("input", partitions)
    val adjustableList = Await.result(adjustableListFuture, DURATION)
    worker.adjustableLists += adjustableList.asInstanceOf[AdjustableList[Any, Any]]
    adjustableList.asInstanceOf[AdjustableList[T, V]]
  }
}
