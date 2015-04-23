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
import scala.concurrent.Future
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
       masterRef: ActorRef): Props =
    Props(classOf[AggregatorModifierActor], conf, workerInfo, datastoreId, masterRef: ActorRef)
}

class AggregatorModifierActor
    (conf: AggregatorListConf,
     workerInfo: WorkerInfo,
     datastoreId: TaskId,
     masterRef: ActorRef)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val dependencies = new DependencyManager()

  private val values = mutable.Map[Any, Any]()

  private val resolver = new Resolver(masterRef)

  def put(key: Any, value: Any): Future[Any] = {
    val futures = mutable.Buffer[Future[Any]]()
    if (conf.valueType.threshold(value)) {
      if (values.contains(key)) {
        values(key) = conf.valueType.aggregator(value, values(key))
        if (values(key) == conf.valueType.initialValue) {
          values -= key
        }
      } else {
        values(key) = value
      }

      futures += dependencies.informDependents(conf.inputId, key)
    }

    Future.sequence(futures)
  }

  def get(key: Any): Any = {
    values(key)
  }

  def getAdjustableList() = new AggregatorList(datastoreId)

  def toBuffer(): mutable.Buffer[(Any, Any)] = {
    val buf = mutable.Buffer[(Any, Any)]()

    for ((key, value) <- values) {
      buf += ((key, value))
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
      sender ! "done"

    case PutAllMessage(values: Iterable[(Any, Any)]) =>
      val futures = values.map {
        case (k, v) => put(k, v)
      }

      Future.sequence(futures) pipeTo sender

    case GetMessage(key: Any, taskRef: ActorRef) =>
      sender ! get(key)
      dependencies.addKeyDependency(conf.inputId, key, taskRef)

    case x =>
      log.warning("AggregatorModifierActor Received unhandled message " + x +
        " from " + sender)
  }
}
