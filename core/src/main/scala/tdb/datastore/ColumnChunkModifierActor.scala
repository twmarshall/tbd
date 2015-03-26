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

import tdb.Mod
import tdb.Constants._
import tdb.list._
import tdb.messages._
import tdb.worker.WorkerInfo
import tdb.util.HashRange

object ColumnChunkModifierActor {
  def props
      (conf: ColumnListConf,
       workerInfo: WorkerInfo,
       datastoreId: DatastoreId,
       range: HashRange): Props =
    Props(classOf[ColumnChunkModifierActor], conf, workerInfo, datastoreId, range)
}

class ColumnChunkModifierActor
    (conf: ColumnListConf,
     workerInfo: WorkerInfo,
     datastoreId: DatastoreId,
     range: HashRange)
  extends Actor with ActorLogging {
  import context.dispatcher

  private val datastore = new Datastore(workerInfo, log, datastoreId)

  // Contains the last DoubleChunkListNode before the tail node. If the list is
  // empty, the contents of this mod will be null.
  private var lastNodeMod = datastore.createMod[ColumnChunkListNode[Any]](null)

  val nodes = mutable.Map[Any, Mod[ColumnChunkListNode[Any]]]()
  val previous = mutable.Map[Any, Mod[ColumnChunkListNode[Any]]]()

  val list = new ColumnChunkList[Any](
    lastNodeMod, conf, false, datastore.workerInfo.workerId)

  def loadInput(keys: Iterable[Any]) = ???

  private def makeNewColumns(column: String, key: Any, value: Any) = {
    val newColumns = mutable.Map[String, Mod[Iterable[Any]]]()

    for ((columnName, (columnType, defaultValue)) <- conf.columns) {
      columnName match {
        case "key" =>
          newColumns("key") = datastore.createMod(Iterable(key))
        case name if name == column =>
          newColumns(columnName) = datastore.createMod(Iterable(value))
        case _ =>
          newColumns(columnName) = datastore.createMod(Iterable(defaultValue))
      }
    }

    newColumns
  }

  private def appendIn(column: String, key: Any, value: Any): Future[_] = {
    val lastNode = datastore.read(lastNodeMod)

    if (lastNode == null) {
      // The list must be empty.
      val newColumns = makeNewColumns(column, key, value)
      val size = 1

      previous(key) = null

      val tailMod = datastore.createMod[ColumnChunkListNode[Any]](null)
      val newNode = new ColumnChunkListNode(newColumns, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val newColumns = makeNewColumns(column, key, value)
      previous(key) = lastNodeMod

      lastNodeMod = lastNode.nextMod

      val tailMod = datastore.createMod[ColumnChunkListNode[Any]](null)
      val newNode = new ColumnChunkListNode(newColumns, tailMod, 1)

      nodes(key) = lastNode.nextMod

      datastore.updateMod(lastNode.nextMod.id, newNode)
    } else {
      val futures = mutable.Buffer[Future[Any]]()
      val oldColumns = lastNode.columns

      for ((columnName, chunkMod) <- oldColumns) {
        columnName match {
          case "key" =>
            val keys = datastore.read(chunkMod)
            previous(key) = previous(keys.head)
            futures += datastore.updateMod(chunkMod.id, keys ++ Iterable(key))
          case name if name == column =>
            val values = datastore.read(chunkMod)
            futures += datastore.updateMod(
              chunkMod.id, values ++ Iterable(value))
          case _ =>
            val values = datastore.read(chunkMod)
            futures += datastore.updateMod(
              chunkMod.id, values ++ Iterable(conf.columns(columnName)._2))
        }
      }
      val size = lastNode.size + 1

      val tailMod = datastore.createMod[ColumnChunkListNode[Any]](null)
      val newNode = new ColumnChunkListNode(oldColumns, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    }
  } //ensuring(isValid())

  def put(key: Any, value: Any): Future[_] = ???

  def putIn(column: String, key: Any, value: Any): Future[_] = {
    if (!nodes.contains(key)) {
      //println("appendIn " + column + " key " + key + " value " + value + " " + this)
      appendIn(column, key, value)
    } else {
      //println("updating " + column + " " + key + " " + value)
      val node = datastore.read(nodes(key))

      val keyIter = datastore.read(node.columns("key")).iterator

      val columnMod = node.columns(column)
      val columnType = conf.columns(column)._1
      val chunk = datastore.read(columnMod)
      var found = false
      val newChunk = chunk.map {
        case _value =>
          val _key = keyIter.next
          if (key == _key) {
            assert(!found)
            found = true

            columnType match {
              case aggregatedColumn: AggregatedColumn =>
                aggregatedColumn.aggregator(_value, value)
              case _ =>
                value
            }
          } else {
            _value
          }
      }
      assert(found)

      //val newNode = new ColumnChunkListNode(node.columns, node.nextMod, node.size)
      //datastore.updateMod(nodes(key).id, newNode)
      datastore.updateMod(columnMod.id, newChunk)
    }
  }

  def get(key: Any): Any = ???

  def remove(key: Any, value: Any): Future[_] = ???

  def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList() = list.asInstanceOf[AdjustableList[Any, Any]]

  def toBuffer(): mutable.Buffer[(Any, Any)] = ???

  def receive = {
    case CreateModMessage(value: Any) =>
      sender ! datastore.createMod(value)

    case CreateModMessage(null) =>
      sender ! datastore.createMod(null)

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

    case UpdateModMessage(modId: ModId, value: Any, task: ActorRef) =>
      datastore.updateMod(modId, value, task) pipeTo sender

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      datastore.updateMod(modId, value, null) pipeTo sender

    case UpdateModMessage(modId: ModId, null, task: ActorRef) =>
      datastore.updateMod(modId, null, task) pipeTo sender

    case UpdateModMessage(modId: ModId, null, null) =>
      datastore.updateMod(modId, null, null) pipeTo sender

    case RemoveModsMessage(modIds: Iterable[ModId], taskRef: ActorRef) =>
      datastore.removeMods(modIds, taskRef) pipeTo sender

    case LoadFileMessage(fileName: String) =>
      datastore.loadPartitions(fileName, range)
      datastore.processKeys {
        case keys => loadInput(keys)
      }
      sender ! "done"

    case GetAdjustableListMessage() =>
      sender ! getAdjustableList()

    case ToBufferMessage() =>
      sender ! toBuffer()

    case PutMessage(key: Any, value: Any) =>
      // This solves a bug where sometimes deserialized Scala objects show up as
      // null in matches. We should figure out a better way of solving this.
      //key.toString
      //value.toString

      val futures = mutable.Buffer[Future[Any]]()
      futures += put(key, value)
      futures += datastore.informDependents(conf.inputId, key)
      Future.sequence(futures) pipeTo sender

    case PutAllMessage(values: Iterable[(Any, Any)]) =>
      val futures = mutable.Buffer[Future[Any]]()
      for ((key, value) <- values) {
        futures += put(key, value)
        futures += datastore.informDependents(conf.inputId, key)
      }
      Future.sequence(futures) pipeTo sender

    case PutInMessage(column: String, key: Any, value: Any) =>
      putIn(column, key, value) pipeTo sender

    case PutAllInMessage(column: String, values: Iterable[(Any, Any)]) =>
      val futures = values.map {
        case (key, value) => putIn(column, key, value)
      }
      Future.sequence(futures) pipeTo sender

    case GetMessage(key: Any, taskRef: ActorRef) =>
      sender ! get(key)
      datastore.addKeyDependency(conf.inputId, key, taskRef)

    case RemoveMessage(key: Any, value: Any) =>
      val futures = mutable.Buffer[Future[Any]]()
      futures += remove(key, value)
      futures += datastore.informDependents(conf.inputId, key)
      Future.sequence(futures) pipeTo sender

    case RemoveAllMessage(values: Iterable[(Any, Any)]) =>
      val futures = mutable.Buffer[Future[Any]]()
      for ((key, value) <- values) {
        futures += remove(key, value)
        futures += datastore.informDependents(conf.inputId, key)
      }
      Future.sequence(futures) pipeTo sender

    case ClearMessage() =>
      datastore.clear()

    case FlushMessage(nodeId: NodeId, taskRef: ActorRef) =>
      sender ! "done"

    case x =>
      log.warning("ColumnChunkModifierActor received unhandled message " + x +
        " from " + sender)
  }
}
