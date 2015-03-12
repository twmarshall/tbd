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

class DoubleChunkListModifier(datastore: Datastore, conf: ListConf)
    (implicit ec: ExecutionContext)
  extends Modifier {

  // Contains the last DoubleChunkListNode before the tail node. If the list is
  // empty, the contents of this mod will be null.
  private var lastNodeMod = datastore.createMod[DoubleChunkListNode[Any, Any]](null)

  val nodes = Map[Any, Mod[DoubleChunkListNode[Any, Any]]]()
  val previous = Map[Any, Mod[DoubleChunkListNode[Any, Any]]]()

  val list = new DoubleChunkList[Any, Any](
      lastNodeMod, conf, false, datastore.workerInfo.workerId)

  val duplicateKeys = Buffer[Any]()

  def loadInput(keys: Iterator[Any]) = {
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
  }// ensuring(isValid())

  private def append(key: Any, value: Any): Future[_] = {
    val lastNode = datastore.read(lastNodeMod)

    if (lastNode == null) {
      // The list must be empty.
      val chunk = Vector[(Any, Any)]((key -> value))
      val size = conf.chunkSizer(value)

      previous(key) = null

      val chunkMod = datastore.createMod(chunk)
      val tailMod = datastore.createMod[DoubleChunkListNode[Any, Any]](null)
      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val chunk = Vector[(Any, Any)]((key -> value))
      previous(key) = lastNodeMod

      lastNodeMod = lastNode.nextMod

      val chunkMod = datastore.createMod(chunk)
      val tailMod = datastore.createMod[DoubleChunkListNode[Any, Any]](null)
      val newNode = new DoubleChunkListNode(
        chunkMod, tailMod, conf.chunkSizer(value))

      nodes(key) = lastNode.nextMod

      datastore.updateMod(lastNode.nextMod.id, newNode)
    } else {
      val oldChunk = datastore.read(lastNode.chunkMod)
      val chunk = oldChunk :+ (key -> value)

      val size = lastNode.size + conf.chunkSizer(value)

      previous(key) = previous(chunk.head._1)
      val chunkMod = datastore.createMod(chunk)
      val tailMod = datastore.createMod[DoubleChunkListNode[Any, Any]](null)
      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)

      nodes(key) = lastNodeMod

      datastore.updateMod(lastNodeMod.id, newNode)
    }

  } //ensuring(isValid())

  private def calculateSize(chunk: Vector[(Any, Any)]) = {
    chunk.aggregate(0)(
      (sum: Int, pair: (Any, Any)) => sum + conf.chunkSizer(pair), _ + _)
  }

  def put(key: Any, value: Any): Future[_] = {
    if (!nodes.contains(key)) {
      append(key, value)
    } else {
      val node = datastore.read(nodes(key))

      var oldValue: Any = null.asInstanceOf[Any]
      val chunk = datastore.read(node.chunkMod)
      val newChunk = chunk.map{ case (_key, _value) => {
        if (key == _key) {
          oldValue = _value
          (_key -> value)
        } else {
          (_key -> _value)
        }
      }}

      val newSize = node.size + conf.chunkSizer(value) -
      conf.chunkSizer(oldValue)
      val chunkMod = datastore.createMod(newChunk)
      val newNode = new DoubleChunkListNode(chunkMod, node.nextMod, newSize)
      datastore.updateMod(nodes(key).id, newNode)
    }
  } //ensuring(isValid())

  def putIn(column: String, key: Any, value: Any): Future[_] = ???

  def get(key: Any): Any = ???

  def remove(key: Any, value: Any): Future[_] = {
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
  } //ensuring(isValid())

  def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  def listSize(): Int = {
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
  }

  def getAdjustableList() = list

  def toBuffer(): Buffer[(Any, Any)] = ???
}
