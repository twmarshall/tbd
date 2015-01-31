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

import scala.collection.mutable.Map
import scala.concurrent.{Await, Future}

import tdb.{Mod, Mutator}
import tdb.Constants._
import tdb.list._

class ChunkListModifier[T, U](datastore: Datastore, conf: ListConf)
    extends ListInput[T, U] {
  protected var tailMod = datastore.createMod[ChunkListNode[T, U]](null)

  // Contains the last ChunkListNode before the tail node. If the list is empty,
  // the contents of this mod will be null.
  protected var lastNodeMod = datastore.createMod[ChunkListNode[T, U]](null)

  val nodes = Map[T, Mod[ChunkListNode[T, U]]]()
  val previous = Map[T, Mod[ChunkListNode[T, U]]]()

  val list = new ChunkList[T, U](lastNodeMod, conf, datastore.workerId)

  def load(data: Map[T, U]) {
    var chunk = Vector[(T, U)]()
    var lastChunk: Vector[(T, U)] = null
    var newLastNodeMod: Mod[ChunkListNode[T, U]] = null

    var size = 0
    var tail = tailMod
    for ((key, value) <- data) {
      chunk :+= ((key, value))
      size += conf.chunkSizer(value)

      if (size >= conf.chunkSize) {
        val newNode = new ChunkListNode(chunk, tail, size)
        tail = datastore.createMod(newNode)
        for ((k, v) <- chunk) {
          nodes(k) = tail
        }

        if (lastChunk != null) {
          for ((k, v) <- lastChunk) {
            previous(k) = tail
          }
        }

        if (newLastNodeMod == null) {
          newLastNodeMod = tail
        }

        lastChunk = chunk
        chunk = Vector[(T, U)]()
        size  = 0
      }
    }

    if (size > 0) {
      for ((k, v) <- chunk) {
        nodes(k) = lastNodeMod
        previous(k) = null
      }

      if (lastChunk != null) {
        for ((k, v) <- lastChunk) {
          previous(k) = lastNodeMod
        }
      }

      datastore.update(lastNodeMod, new ChunkListNode(chunk, tail, size))
    } else {
      val head = datastore.read(tail)
      for ((k, v) <- head.chunk) {
        nodes(k) = lastNodeMod
        previous(k) = null
      }

      datastore.update(lastNodeMod, head)
    }

    lastNodeMod = newLastNodeMod
  }

  def put(key: T, value: U) {
    Await.result(asyncPut(key, value), DURATION)
  }

  def asyncPut(key: T, value: U): Future[_] = {
    val lastNode = datastore.read(lastNodeMod)

    val newNode =
      if (lastNode == null) {
        val chunk = Vector[(T, U)]((key -> value))
        val size = conf.chunkSizer(value)

        previous(key) = null

        new ChunkListNode(chunk, tailMod, size)
      } else if (lastNode.size >= conf.chunkSize) {
        val newTailMod = datastore.createMod[ChunkListNode[T, U]](null)
        val chunk = Vector[(T, U)]((key -> value))
        previous(key) = lastNodeMod

        lastNodeMod = tailMod
        tailMod = newTailMod

        new ChunkListNode(chunk, newTailMod, conf.chunkSizer(value))
      } else {
        val chunk = lastNode.chunk :+ (key -> value)
        val size = lastNode.size + conf.chunkSizer(value)

        previous(key) = previous(chunk.head._1)
        new ChunkListNode(chunk, tailMod, size)
      }

    nodes(key) = lastNodeMod

    datastore.asyncUpdate(lastNodeMod, newNode)
  } //ensuring(isValid())

  private def calculateSize(chunk: Vector[(T, U)]) = {
    chunk.aggregate(0)(
      (sum: Int, pair: (T, U)) => sum + conf.chunkSizer(pair), _ + _)
  }

  def update(key: T, value: U) {
    val node = datastore.read(nodes(key))

    var oldValue: U = null.asInstanceOf[U]
    val newChunk = node.chunk.map{ case (_key, _value) => {
      if (key == _key) {
        oldValue = _value
        (_key -> value)
      } else {
        (_key -> _value)
      }
    }}

    val newSize = node.size + conf.chunkSizer(value) - conf.chunkSizer(oldValue)
    val newNode = new ChunkListNode(newChunk, node.nextMod, newSize)

    datastore.update(nodes(key), newNode)
  } //ensuring(isValid())

  def remove(key: T, value: U) {
    val node = datastore.read(nodes(key))

    var oldValue: U = null.asInstanceOf[U]
    val newChunk = node.chunk.filter{ case (_key, _value) => {
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
            tailMod = nodes(key)
          }
        } else if (lastNodeMod.id == node.nextMod.id) {
          // We are removing the second to last node.
          lastNodeMod = nodes(key)
        }

        if (nextNode != null) {
          for ((k, v) <- nextNode.chunk) {
            nodes(k) = nodes(key)
            previous(k) = previous(key)
          }

          val nextNextNode = datastore.read(nextNode.nextMod)
          if (nextNextNode != null) {
            for ((k, v) <- nextNextNode.chunk) {
              previous(k) = nodes(key)
            }
          }
        }

        nextNode
      } else {
        val newSize = node.size - conf.chunkSizer(oldValue)
        new ChunkListNode(newChunk, node.nextMod, newSize)
      }

    datastore.update(nodes(key), newNode)
    nodes -= key
  } //ensuring(isValid())

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  private def isValid(): Boolean = {
    var previousMod: Mod[ChunkListNode[T, U]] = null
    var previousNode: ChunkListNode[T, U] = null
    var mod = list.head
    var node = datastore.read(mod)

    var valid = true
    while (node != null) {
      for ((key, value) <- node.chunk) {
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

    valid
  }

  def getAdjustableList() = list
}
