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

class DoubleChunkListModifier[T, U](datastore: Datastore, conf: ListConf)
    extends ListInput[T, U] {
  protected var tailMod = datastore.createMod[DoubleChunkListNode[T, U]](null)

  // Contains the last DoubleChunkListNode before the tail node. If the list is empty,
  // the contents of this mod will be null.
  protected var lastNodeMod = datastore.createMod[DoubleChunkListNode[T, U]](null)

  val nodes = Map[T, Mod[DoubleChunkListNode[T, U]]]()
  val previous = Map[T, Mod[DoubleChunkListNode[T, U]]]()

  val list = new DoubleChunkList[T, U](lastNodeMod, conf, false, datastore.workerId)

  def load(data: Map[T, U]) {
    var chunk = Vector[(T, U)]()
    var lastChunk: Vector[(T, U)] = null
    var newLastNodeMod: Mod[DoubleChunkListNode[T, U]] = null

    var size = 0
    var tail = tailMod
    for ((key, value) <- data) {
      chunk :+= ((key, value))
      size += conf.chunkSizer(value)

      if (size >= conf.chunkSize) {
        val chunkMod = datastore.createMod(chunk)
        val newNode = new DoubleChunkListNode(chunkMod, tail, size)
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

      val chunkMod = datastore.createMod(chunk)
      datastore.update(lastNodeMod, new DoubleChunkListNode(chunkMod, tail, size))
    } else {
      val head = datastore.read(tail)
      val chunk = datastore.read(head.chunkMod)
      for ((k, v) <- chunk) {
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

        val chunkMod = datastore.createMod(chunk)
        new DoubleChunkListNode(chunkMod, tailMod, size)
      } else if (lastNode.size >= conf.chunkSize) {
        val newTailMod = datastore.createMod[DoubleChunkListNode[T, U]](null)
        val chunk = Vector[(T, U)]((key -> value))
        previous(key) = lastNodeMod

        lastNodeMod = tailMod
        tailMod = newTailMod

        val chunkMod = datastore.createMod(chunk)
        new DoubleChunkListNode(chunkMod, newTailMod, conf.chunkSizer(value))
      } else {
        val oldChunk = datastore.read(lastNode.chunkMod)
        val chunk = oldChunk :+ (key -> value)
        val size = lastNode.size + conf.chunkSizer(value)

        previous(key) = previous(chunk.head._1)
        val chunkMod = datastore.createMod(chunk)
        new DoubleChunkListNode(chunkMod, tailMod, size)
      }

    nodes(key) = lastNodeMod

    datastore.asyncUpdate(lastNodeMod, newNode)
  } //ensuring(isValid())

  private def calculateSize(chunk: Vector[(T, U)]) = {
    chunk.aggregate(0)(
      (sum: Int, pair: (T, U)) => sum + conf.chunkSizer(pair), _ + _)
  }

  def putAfter(key: T, newPair: (T, U)) {
    val insertIntoMod = nodes(key)
    val insertInto = datastore.read(nodes(key))
    val newSize = insertInto.size + conf.chunkSizer(newPair._2)

    var found = false
    val chunk = datastore.read(insertInto.chunkMod)
    val newChunk = chunk.flatMap((pair: (T, U)) => {
      if (pair._1 == key) {
        found = true
        Vector(pair, newPair)
      } else {
        Vector(pair)
      }
    })

    val newNode =
      if (newSize > conf.chunkSize) {
        val (newChunk1, newChunk2) = newChunk.splitAt(chunk.size / 2)

        val size1 = calculateSize(newChunk1)
        val size2 = calculateSize(newChunk2)

        val chunkMod2 = datastore.createMod(newChunk2)
        val newNode2 =
          new DoubleChunkListNode(chunkMod2, insertInto.nextMod, size2)
        val newNodeMod2 = datastore.createMod(newNode2)

        newChunk2.foreach {
          case (_key, _value) =>
            nodes(_key) = newNodeMod2
            previous(_key) = insertIntoMod
        }

        if (newChunk1.contains(newPair)) {
          nodes(newPair._1) = insertIntoMod
          previous(newPair._1) = previous(chunk.head._1)
        }

        val chunkMod1 = datastore.createMod(newChunk1)
        new DoubleChunkListNode(chunkMod1, newNodeMod2, size1)
      } else {
        nodes(newPair._1) = insertIntoMod
        previous(newPair._1) = previous(chunk.head._1)

        val chunkMod = datastore.createMod(newChunk)
        new DoubleChunkListNode(
          chunkMod,
          insertInto.nextMod,
          insertInto.size + conf.chunkSizer(newPair))
      }

    datastore.update(insertIntoMod, newNode)
  }

  def update(key: T, value: U) {
    val node = datastore.read(nodes(key))

    var oldValue: U = null.asInstanceOf[U]
    val chunk = datastore.read(node.chunkMod)
    val newChunk = chunk.map{ case (_key, _value) => {
      if (key == _key) {
        oldValue = _value
        (_key -> value)
      } else {
        (_key -> _value)
      }
    }}

    val newSize = node.size + conf.chunkSizer(value) - conf.chunkSizer(oldValue)
    val chunkMod = datastore.createMod(newChunk)
    val newNode = new DoubleChunkListNode(chunkMod, node.nextMod, newSize)

    datastore.update(nodes(key), newNode)
  } //ensuring(isValid())

  def remove(key: T, value: U) {
    val node = datastore.read(nodes(key))

    var oldValue: U = null.asInstanceOf[U]
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
            tailMod = nodes(key)
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

    datastore.update(nodes(key), newNode)
    nodes -= key
  } //ensuring(isValid())

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  private def isValid(): Boolean = {
    var previousMod: Mod[DoubleChunkListNode[T, U]] = null
    var previousNode: DoubleChunkListNode[T, U] = null
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

    valid
  }

  def getAdjustableList() = list
}
