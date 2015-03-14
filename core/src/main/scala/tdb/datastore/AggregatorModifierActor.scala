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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.pipe
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

import tdb.Constants._
import tdb.list._
import tdb.messages._
import tdb.worker.WorkerInfo
import tdb.util.HashRange

object AggregatorModifierActor {
  def props
      (conf: AggregatorListConf[Any],
       workerInfo: WorkerInfo,
       datastoreId: DatastoreId): Props =
    Props(classOf[AggregatorModifierActor], conf, workerInfo, datastoreId)
}

class AggregatorModifierActor
    (conf: AggregatorListConf[Any],
     workerInfo: WorkerInfo,
     datastoreId: DatastoreId)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val datastore = new Datastore(workerInfo, log, datastoreId)

  private val dummyFuture = Future { "done" }

  private val values = mutable.Map[Any, Any]()

  private val buffer = mutable.Map[Any, Any]()

  def put(key: Any, value: Any) {
    if (buffer.contains(key)) {
      val oldValue = buffer(key)
      val newValue = conf.aggregator(oldValue, value)
      buffer(key) = newValue
    } else {
      buffer(key) = value
    }
  }

  def get(key: Any): Any = {
    values(key)
  }

  def remove(key: Any, value: Any) {
    val oldValue = buffer(key)

    var newValue = conf.deaggregator(value, oldValue)

    buffer(key) = newValue
  }

  def getAdjustableList() = new AggregatorList(self)

  def toBuffer(): mutable.Buffer[(Any, Any)] = {
    val buf = mutable.Buffer[(Any, Any)]()

    for ((key, value) <- values) {
      buf += ((key, value))
    }

    buf
  }

  def receive = {

    case LoadPartitionsMessage
        (fileName: String,
         numWorkers: Int,
         workerIndex: Int,
         partitions: Int) =>
      log.debug("LoadPartitionsMessage")
      val range = new HashRange(
        workerIndex * partitions,
        (workerIndex + 1) * partitions,
        numWorkers * partitions)

      datastore.loadPartitions(fileName, range)

      sender ! "done"

    case GetAdjustableListMessage() =>
      sender ! getAdjustableList()

    case ToBufferMessage() =>
      sender ! toBuffer()

    case PutMessage(key: Any, value: Any) =>
      put(key, value)
      sender ! "done"

    case PutAllMessage(values: Iterable[(Any, Any)]) =>
      values.map {
        case (k, v) => put(k, v)
      }
      sender ! "done"

    case GetMessage(key: Any, taskRef: ActorRef) =>
      sender ! get(key)
      datastore.addKeyDependency(conf.inputId, key, taskRef)

    case RemoveMessage(key: Any, value: Any) =>
      remove(key, value)
      sender ! "done"

    case RemoveAllMessage(values: Iterable[(Any, Any)]) =>
      values.map {
        case (k, v) => remove(k, v)
      }
      sender ! "done"

    case ClearMessage() =>
      datastore.clear()

    case FlushMessage() =>
      val futures = mutable.Buffer[Future[Any]]()
      for ((key, value) <- buffer) {
        if (conf.threshold(value)) {
          if (values.contains(key)) {
            values(key) = conf.aggregator(value, values(key))
          } else {
            values(key) = value
          }
          futures += datastore.informDependents(conf.inputId, key)
        }
      }
      Future.sequence(futures) pipeTo sender

    case x => log.warning("Received unhandled message " + x + " from " + sender)
  }
}
