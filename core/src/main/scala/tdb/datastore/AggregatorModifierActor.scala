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

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern.{ask, pipe}
import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success}

import tdb.Resolver
import tdb.Constants._
import tdb.list._
import tdb.messages._
import tdb.worker.WorkerInfo
import tdb.util.HashRange

object AggregatorModifierActor {
  def props
      (conf: AggregatorListConf,
       workerInfo: WorkerInfo,
       datastoreId: TaskId,
       masterRef: ActorRef,
       recovery: Boolean): Props =
    Props(
      classOf[AggregatorModifierActor],
      conf, workerInfo, datastoreId, masterRef, recovery)
}

class AggregatorModifierActor
    (conf: AggregatorListConf,
     workerInfo: WorkerInfo,
     datastoreId: TaskId,
     masterRef: ActorRef,
     recovery: Boolean)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val store = KVStore(workerInfo)

  private val tableId = {
    val tableName =  "datastore-" + datastoreId
    val valueType = conf.valueType.columnType
    store.createTable(tableName, "String", valueType, null, !recovery)
  }

  private val dependencies = new DependencyManager()

  private val resolver = new Resolver(masterRef)

  def put(key: Any, value: Any): Future[Any] = {
    val futures = mutable.Buffer[Future[Any]]()

    if (conf.valueType.threshold(value)) {
      if (store.contains(tableId, key)) {
        val oldValue = Await.result(store.get(tableId, key), DURATION)
        val newValue = conf.valueType.aggregator(value, oldValue)

        if (newValue == conf.valueType.initialValue) {
          store.delete(tableId, key)
        } else {
          futures += store.put(tableId, key, newValue)
        }
      } else {
        futures += store.put(tableId, key, value)
      }

      futures += dependencies.informDependents(conf.inputId, key)
    }

    Future.sequence(futures)
  }

  def getAdjustableList() = new AggregatorList(datastoreId)

  def toBuffer(): mutable.Buffer[(Any, Any)] = {
    val buf = mutable.Buffer[(Any, Any)]()

    store.foreach(tableId) {
      case (key, value) => buf += ((key, value))
    }

    buf
  }

  def receive = {

    case GetAdjustableListMessage() =>
      sender ! getAdjustableList()

    case ToBufferMessage() =>
      sender ! toBuffer()

    case PutMessage(table: String, key: Any, value: Any, taskRef) =>
      put(key, value) pipeTo sender

    case PutAllMessage(values: Iterable[(Any, Any)]) =>
      val futures = values.map {
        case (key, value) => put(key, value)
      }

      Future.sequence(futures) pipeTo sender

    case GetMessage(key: Any, taskRef: ActorRef) =>
      store.get(tableId, key) pipeTo sender
      dependencies.addKeyDependency(conf.inputId, key, taskRef)

    case "ping" => sender ! "done"

    case x =>
      log.warning("AggregatorModifierActor Received unhandled message " + x +
        " from " + sender)
  }
}
