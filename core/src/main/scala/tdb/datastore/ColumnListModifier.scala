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

import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{ExecutionContext, Future}

import tdb.{Mod, Mutator}
import tdb.Constants._
import tdb.list._

class ColumnListModifier(datastore: Datastore, conf: ColumnListConf)
    (implicit ec: ExecutionContext)
  extends Modifier {

  // Contains the last DoubleChunkListNode before the tail node. If the list is
  // empty, the contents of this mod will be null.
  private var lastNodeMod = datastore.createMod[ColumnListNode[Any]](null)

  val nodes = Map[Any, Mod[ColumnListNode[Any]]]()
  val previous = Map[Any, Mod[ColumnListNode[Any]]]()

  val list =
    new ColumnList[Any](lastNodeMod, conf, false, datastore.workerInfo.workerId)

  def loadInput(keys: Iterable[Any]) = ???/*{
    val futures = Buffer[Future[Any]]()

    var chunk = Vector[Any]()
    var lastChunk: Vector[Any] = null
    var secondToLastChunk: Vector[Any] = null

    var size = 0

    var lastNode = datastore.read(lastNodeMod)

    val oldHead = datastore.read(list.head)
    var tail = datastore.createMod(oldHead)
    for (key <- keys) {
      chunk :+= key
      size += 1

      if (size >= conf.chunkSize) {
        val chunkMod = new Mod[Vector[(Any, Any)]](datastore.getNewModId())
        datastore.chunks(chunkMod.id) = chunk
        val newNode = new DoubleChunkListNode(chunkMod, tail, size)

        tail = datastore.createMod(newNode)
        for (k <- chunk) {
          nodes(k) = tail
        }

        if (lastChunk != null) {
          for (k <- lastChunk) {
            previous(k) = tail
          }
        }

        if (lastNode == null) {
          lastNodeMod = tail
          lastNode = newNode
        }

        secondToLastChunk = lastChunk
        lastChunk = chunk
        chunk = Vector[Any]()
        size  = 0
      }
    }

    if (size > 0) {
      for (k <- chunk) {
        nodes(k) = list.head
        previous(k) = null
      }

      if (lastChunk != null) {
        for (k <- lastChunk) {
          previous(k) = list.head
        }
      }

      val chunkMod = new Mod[Vector[(Any, Any)]](datastore.getNewModId())
      datastore.chunks(chunkMod.id) = chunk
      futures += datastore.updateMod(
        list.head.id, new DoubleChunkListNode(chunkMod, tail, size))
    } else {
      val head = datastore.read(tail)

      if (head != null) {
        val chunk = datastore.read(head.chunkMod)
        for ((k, v) <- chunk) {
          nodes(k) = list.head
          previous(k) = null
        }

        if (secondToLastChunk != null) {
          for (k <- secondToLastChunk) {
            previous(k) = list.head
          }
        }

        futures +=
          datastore.updateMod(list.head.id, head)
      }
    }

    // This means there is only one chunk in the list.
    if (lastNode == null) {
      lastNodeMod = list.head
    }

    Future.sequence(futures)
  }// ensuring(isValid())*/


  private def makeNewColumns(column: String, key: Any, value: Any) = {
    val newColumns = Map[String, Mod[Iterable[Any]]]()

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

      val tailMod = datastore.createMod[ColumnListNode[Any]](null)
      val newNode = new ColumnListNode(newColumns, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val newColumns = makeNewColumns(column, key, value)
      previous(key) = lastNodeMod

      lastNodeMod = lastNode.nextMod

      val tailMod = datastore.createMod[ColumnListNode[Any]](null)
      val newNode = new ColumnListNode(newColumns, tailMod, 1)

      nodes(key) = lastNode.nextMod

      datastore.updateMod(lastNode.nextMod.id, newNode)
    } else {
      val futures = Buffer[Future[Any]]()
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

      val tailMod = datastore.createMod[ColumnListNode[Any]](null)
      val newNode = new ColumnListNode(oldColumns, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    }
  } //ensuring(isValid())

  def put(key: Any, value: Any): Future[_] = ???

  def putIn(column: String, key: Any, value: Any): Future[_] = {
    if (!nodes.contains(key)) {
      println("appendIn " + column + " key " + key + " value " + value + " " + this)
      appendIn(column, key, value)
    } else {
      println("updating " + column + " " + key + " " + value)
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
              case _ => ???//value
            }
          } else {
            _value
          }
      }
      assert(found)

      //val newNode = new ColumnListNode(node.columns, node.nextMod, node.size)
      //datastore.updateMod(nodes(key).id, newNode)
      datastore.updateMod(columnMod.id, newChunk)
    }
  }

  def get(key: Any): Any = ???

  def remove(key: Any, value: Any): Future[_] = ???/*{
    val node = datastore.read(nodes(key))

    var oldValue: Any = null.asInstanceOf[Any]
    val chunk = datastore.read(node.chunkMod)
    val newChunk = chunk.filter{ case (_key, _value) => {
      if (key == _key) {
        oldValue = _value
        false
      } else {
        true
      }
    }}

    val newNode =
      if (newChunk.size == 0) {
        val nextNode = datastore.read(node.nextMod)

        if (nextNode == null) {
          if (previous(key) == null) {
            // We're removing the last element in the list.
            lastNodeMod = list.head
          } else {
            // We are removing the node at the end list.
            lastNodeMod = previous(key)
          }
        } else if (lastNodeMod.id == node.nextMod.id) {
          // We are removing the second to last node.
          lastNodeMod = nodes(key)
        }

        if (nextNode != null) {
          val nextChunk = datastore.read(nextNode.chunkMod)
          for ((k, v) <- nextChunk) {
            nodes(k) = nodes(key)
            previous(k) = previous(key)
          }

          val nextNextNode = datastore.read(nextNode.nextMod)
          if (nextNextNode != null) {
            val nextNextChunk = datastore.read(nextNextNode.chunkMod)
            for ((k, v) <- nextNextChunk) {
              previous(k) = nodes(key)
            }
          }
        }

        nextNode
      } else {
        val newSize = node.size - conf.chunkSizer(oldValue)
        val chunkMod = datastore.createMod(newChunk)
        new DoubleChunkListNode(chunkMod, node.nextMod, newSize)
      }

    val future = datastore.updateMod(nodes(key).id, newNode)

    nodes -= key

    future
  } //ensuring(isValid())*/

  def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  /*def listSize(): Int = {
    var node = datastore.read(list.head)

    var size = 0
    while (node != null) {
      val chunk = datastore.read(node.chunkMod)
      size += chunk.size
      node = datastore.read(node.nextMod)
    }

    size
  }

  private def isValid(): Boolean = {
    //println("isValid")
    var previousMod: Mod[DoubleChunkListNode[Any, Any]] = null
    var previousNode: DoubleChunkListNode[Any, Any] = null
    var mod = list.head
    var node = datastore.read(mod)

    var valid = true
    while (node != null) {
      val chunk = datastore.read(node.chunkMod)
      for ((key, value) <- chunk) {
        if (previousMod == null)
          valid &= previous(key) == null
        else
          valid &= previous(key) == previousMod

        valid &= nodes(key) == mod
      }

      previousMod = mod
      previousNode = node
      mod = node.nextMod
      node = datastore.read(mod)
    }

    if (previousMod != null) {
      valid &= previousMod == lastNodeMod
    }
    //println("done")
    valid
  }*/

  def getAdjustableList() = list.asInstanceOf[AdjustableList[Any, Any]]

  def toBuffer(): Buffer[(Any, Any)] = ???
}
