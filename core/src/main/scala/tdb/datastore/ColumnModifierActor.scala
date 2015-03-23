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
import akka.pattern.{ask, pipe}
import scala.collection.mutable
import scala.concurrent.Future
import scala.util.{Failure, Success}

import tdb.Mod
import tdb.Constants._
import tdb.list._
import tdb.messages._
import tdb.worker.WorkerInfo
import tdb.util.HashRange

object ColumnModifierActor {
  def props
      (conf: ColumnListConf,
       workerInfo: WorkerInfo,
       datastoreId: DatastoreId,
       range: HashRange): Props =
    Props(classOf[ColumnModifierActor], conf, workerInfo, datastoreId, range)

  var count = 0
}

class ColumnModifierActor
    (conf: ColumnListConf,
     workerInfo: WorkerInfo,
     datastoreId: DatastoreId,
     range: HashRange)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val datastore = new Datastore(workerInfo, log, datastoreId)

  private var tailMod = datastore.createMod[ColumnListNode[Any]](null)

  private val nodes = mutable.Map[Any, Mod[ColumnListNode[Any]]]()

  private val modList = new ColumnList[Any](
    tailMod, conf, false, datastore.workerInfo.workerId)

  private val buffer = mutable.Map[String, mutable.Map[Any, Any]]()

  private def makeNewColumns(column: String, key: Any, value: Any) = {
    val newColumns = mutable.Map[String, Mod[Any]]()

    for ((columnName, (columnType, defaultValue)) <- conf.columns) {
      columnName match {
        case "key" =>
          newColumns("key") = datastore.createMod(key)
        case name if name == column =>
          newColumns(columnName) = datastore.createMod(value)
        case _ =>
          newColumns(columnName) = datastore.createMod(defaultValue)
      }
    }

    newColumns
  }

  private def appendIn(column: String, key: Any, value: Any): Future[_] = {
    val newColumns = makeNewColumns(column, key, value)
    val newTail = datastore.createMod[ColumnListNode[Any]](null)
    val newNode = new ColumnListNode(newColumns, newTail)

    val future = datastore.updateMod(tailMod.id, newNode)

    nodes(key) = tailMod

    tailMod = newTail

    future
  }

  private def putIn(column: String, key: Any, value: Any): Future[_] = {
    if (!nodes.contains(key)) {
      appendIn(column, key, value)
    } else {
      val node = datastore.read(nodes(key))

      val columnMod = node.columns(column)
      val columnType = conf.columns(column)._1
      val _value = datastore.read(columnMod)
      val newValue = columnType match {
        case aggregatedColumn: AggregatedColumn =>
          aggregatedColumn.aggregator(_value, value)
        case _ => value
      }

      datastore.updateMod(columnMod.id, newValue)
    }
  }

  private def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  private def flush(): Future[Any] = {
    val futures = mutable.Buffer[Future[Any]]()
    for ((column, values) <- buffer) {
      for ((key, value) <- values) {
        ColumnModifierActor.count -= 1
        futures += putIn(column, key, value)
      }
    }
    buffer.clear()
    Future.sequence(futures)
  }

  private val flushNodes = mutable.Map[String, (NodeId, ActorRef)]()

  private var flushNotified = false

  def receive = {
    case GetModMessage(modId: ModId, taskRef: ActorRef) =>
      val respondTo = sender
      datastore.getMod(modId, taskRef).onComplete {
        case Success(v) =>
          if (v == null) {
            respondTo ! NullMessage
          } else {
            respondTo ! v
          }
        case Failure(e) => e.printStackTrace()
      }

      datastore.addDependency(modId, taskRef)

    case GetModMessage(modId: ModId, null) =>
      val respondTo = sender
      datastore.getMod(modId, null).onComplete {
        case Success(v) =>
          if (v == null) {
            respondTo ! NullMessage
          } else {
            respondTo ! v
          }
        case Failure(e) => e.printStackTrace()
      }

    case GetAdjustableListMessage() =>
      sender ! modList

    case PutInMessage(column: String, key: Any, value: Any) =>
      putIn(column, key, value) pipeTo sender

    case PutAllInMessage(column: String, values: Iterable[(Any, Any)]) =>
      if (!buffer.contains(column)) {
        buffer(column) = mutable.Map[Any, Any]()
      }

      for ((key, value) <- values) {

        val columnType = conf.columns(column)._1
        columnType match {
          case agg: AggregatedColumn =>
            ColumnModifierActor.count += 1
            buffer(column)(key) = agg.aggregator(
              buffer(column).getOrElse(key, agg.initialValue), value)
          case _ =>
            buffer(column)(key) = value
        }
      }

      if (!flushNotified && flushNodes.contains(column)) {
        val (flushNode, flushTask) = flushNodes(column)
        flushNotified = true
        (flushTask ? NodeUpdatedMessage(flushNode)) pipeTo sender
      } else {
        sender ! "done"
      }

    case ClearMessage() =>
      datastore.clear()

    case FlushMessage(nodeId: NodeId, taskRef: ActorRef) =>
      for ((column, pair) <- buffer) {
        assert(!flushNodes.contains(column))
        flushNodes(column) = (nodeId, taskRef)
      }

      flushNotified = false
      flush() pipeTo sender

    case FlushMessage(nodeId: NodeId, null) =>
      flushNotified = false
      flush() pipeTo sender

    case x =>
      log.warning("ModifierActor received unhandled message " + x +
        " from " + sender)
  }
}
