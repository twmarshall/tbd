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

class AggregatorInput[T, U]
    (val inputId: InputId,
     val hasher: ObjHasher[ActorRef],
     val conf: AggregatorListConf,
     val workers: Iterable[ActorRef])
  extends HashPartitionedListInput[T, U] with java.io.Serializable {

  def getAdjustableList(): AdjustableList[T, U] = {
    val adjustablePartitions = Buffer[AggregatorList[T, U]]()

    for (datastoreRef <- hasher.objs.values) {
      val future = datastoreRef ? GetAdjustableListMessage()
      adjustablePartitions +=
        Await.result(future.mapTo[AggregatorList[T, U]], DURATION)
    }

    new PartitionedAggregatorList(adjustablePartitions, conf)
  }

  override def getBuffer() = new AggregatorBuffer(this, conf)

  override def flush(nodeId: NodeId, taskRef: ActorRef): Unit = {
    val futures = hasher.objs.values.map(
      _ ? FlushMessage(nodeId, taskRef: ActorRef))
    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(Future.sequence(futures), DURATION)
  }
}

class AggregatorBuffer[T, U]
    (input: ListInput[T, U], conf: AggregatorListConf)
  extends InputBuffer[T, U] {

  private val toPut = Map[T, U]()

  def putAll(values: Iterable[(T, U)]) {
    for ((key, value) <- values) {
      if (toPut.contains(key)) {
        toPut(key) = conf.valueType.aggregator(toPut(key), value)
          .asInstanceOf[U]
      } else {
        toPut(key) = value
      }
    }
  }

  def putAllIn(column: String, values: Iterable[(T, Any)]) = ???

  def removeAll(values: Iterable[(T, U)]) {
    for ((key, value) <- values) {
      if (toPut.contains(key)) {
        toPut(key) = conf.valueType.deaggregator(toPut(key), value)
          .asInstanceOf[U]
      } else {
        toPut(key) = conf.valueType.deaggregator(
          conf.valueType.initialValue, value).asInstanceOf[U]
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
