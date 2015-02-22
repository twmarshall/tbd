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
import scala.concurrent.{Await, Future}

import tdb.Constants._
import tdb.messages._
import tdb.util.ObjHasher

class AggregatorInput[T]
    (val hasher: ObjHasher[(String, ActorRef)], conf: ListConf)
  extends HashPartitionedListInput[T, Int] with java.io.Serializable {

  def getAdjustableList(): AdjustableList[T, Int] = {
    val adjustablePartitions = Buffer[DoubleChunkList[T, Int]]()

    for ((listId, datastoreRef) <- hasher.objs.values.toSet) {
      val future = datastoreRef ? GetAdjustableListMessage(listId)
      adjustablePartitions +=
        Await.result(future.mapTo[DoubleChunkList[T, Int]], DURATION)
    }

    new HashPartitionedDoubleChunkList(adjustablePartitions, conf)
  }

  override def getBuffer() = new AggregatorBuffer(this)
}

class AggregatorBuffer[T](input: ListInput[T, Int]) extends InputBuffer[T, Int] {

  private val toPut = Map[T, Int]()

  def putAll(values: Iterable[(T, Int)]) {
    for ((key, value) <- values) {
      if (toPut.contains(key)) {
        toPut(key) += value.asInstanceOf[Int]
      } else {
        toPut(key) = value.asInstanceOf[Int]
      }
    }
  }

  def removeAll(values: Iterable[(T, Int)]) {
    for ((key, value) <- values) {
      if (toPut.contains(key)) {
        toPut(key) -= value.asInstanceOf[Int]
      } else {
        toPut(key) = -value.asInstanceOf[Int]
      }
    }
  }

  def flush() {
    val futures = Buffer[Future[Any]]()

    futures += input.asyncPutAll(toPut)

    toPut.clear()

    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(Future.sequence(futures), DURATION)
  }
}
