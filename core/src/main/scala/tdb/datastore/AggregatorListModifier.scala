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

class AggregatorListModifier(datastore: Datastore, conf: ListConf)
    (implicit ec: ExecutionContext)
  extends Modifier {

  // Contains the last DoubleChunkListNode before the tail node. If the list is
  // empty, the contents of this mod will be null.
  private var lastNodeMod = datastore
    .createMod[DoubleChunkListNode[Any, Int]](null)

  val nodes = Map[Any, Mod[DoubleChunkListNode[Any, Int]]]()
  val previous = Map[Any, Mod[DoubleChunkListNode[Any, Int]]]()

  val list = new DoubleChunkList[Any, Int](
    lastNodeMod, conf, false, datastore.workerId)

  def loadInput(keys: Iterator[Any]) = ???

  private def append(key: Any, value: Any): Future[_] = {
    val lastNode = datastore.read(lastNodeMod)

    if (lastNode == null) {
      // The list must be empty.
      val chunk = Vector[(Any, Int)]((key -> value.asInstanceOf[Int]))
      val size = 1

      previous(key) = null

      val chunkMod = datastore.createMod(chunk)
      val tailMod = datastore.createMod[DoubleChunkListNode[Any, Int]](null)
      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val chunk = Vector[(Any, Int)]((key -> value.asInstanceOf[Int]))
      previous(key) = lastNodeMod

      lastNodeMod = lastNode.nextMod

      val chunkMod = datastore.createMod(chunk)
      val tailMod = datastore.createMod[DoubleChunkListNode[Any, Int]](null)
      val newNode = new DoubleChunkListNode(chunkMod, tailMod, 1)

      nodes(key) = lastNode.nextMod

      datastore.updateMod(lastNode.nextMod.id, newNode)
    } else {
      val oldChunk = datastore.read(lastNode.chunkMod)
      val chunk = oldChunk :+ (key -> value.asInstanceOf[Int])

      val size = lastNode.size + 1

      previous(key) = previous(chunk.head._1)
      val chunkMod = datastore.createMod(chunk)
      val tailMod = datastore.createMod[DoubleChunkListNode[Any, Int]](null)
      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    }

  }

  def put(key: Any, value: Any): Future[_] = {
    if (!nodes.contains(key)) {
      append(key, value)
    } else {
      val node = datastore.read(nodes(key))

      val chunk = datastore.read(node.chunkMod)
      val newChunk = chunk.map {
        case (_key: Any, _value: Int) => {
          if (key == _key) {
            (_key, _value + value.asInstanceOf[Int])
          } else {
            (_key, _value)
          }
        }
      }

      val chunkMod = datastore.createMod(newChunk)
      val newNode = new DoubleChunkListNode(chunkMod, node.nextMod, node.size)
      datastore.updateMod(nodes(key).id, newNode)
    }
  }

  def remove(key: Any, value: Any): Future[_] = {
    val node = datastore.read(nodes(key))

    val chunk = datastore.read(node.chunkMod)
    var removed = false
    val newChunk = chunk.flatMap{ case (_key, _value) => {
      if (key == _key) {
        val newValue = _value - value.asInstanceOf[Int]

        if (newValue == 0) {
          removed = true
          Iterable()
        } else {
          Iterable((_key, newValue))
        }
      } else {
        Iterable((_key, _value))
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
        val newSize = node.size - 1
        val chunkMod = datastore.createMod(newChunk)
        new DoubleChunkListNode(chunkMod, node.nextMod, newSize)
      }

    val future = datastore.updateMod(nodes(key).id, newNode)

    if (removed) {
      nodes -= key
    }

    future
  }

  def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList() = list.asInstanceOf[AdjustableList[Any, Any]]
}
