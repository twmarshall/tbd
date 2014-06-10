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
package tbd.datastore

import akka.actor.ActorRef
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.Future

import tbd.ListConf
import tbd.mod.{ChunkList, ChunkListNode, Mod}

class ChunkListModifier[T, U](
    _datastore: Datastore,
    conf: ListConf) extends Modifier[T, U](_datastore) {
  private var tailMod: Mod[ChunkListNode[T, U]] = null

  // Contains the last ChunkListNode before the tail node.
  private var lastNodeMod: Mod[ChunkListNode[T, U]] = null

  val list = initialize()

  private def initialize(): ChunkList[T, U] = {
    var tail = datastore.createMod[ChunkListNode[T, U]](null)
    tailMod = tail

    tail = datastore.createMod(new ChunkListNode(Vector[(T, U)](), tail, 0))
    lastNodeMod = tail

    var chunk = Vector[(T, U)]()
    var size = 0
    /*for (elem <- table) {
      if (size >= chunkSize) {
	tail = datastore.createMod(new ChunkListNode(chunk, tail, size))

	chunk = Vector[(T, U)]()
	size = 0
      }

      chunk = chunk :+ (elem._1.asInstanceOf[T] -> elem._2.asInstanceOf[U])
      size += chunkSizer(elem._2.asInstanceOf[U])
    }*/

    if (size > 0) {
      tail = datastore.createMod(new ChunkListNode(chunk, tail, size))
    }

    new ChunkList[T, U](tail)
  }

  def insert(key: T, value: U): ArrayBuffer[Future[String]] = {
    val lastNode = datastore.getMod(lastNodeMod.id)
      .asInstanceOf[ChunkListNode[T, U]]

    var futures = ArrayBuffer[Future[String]]()
    if (lastNode == null) {
      val chunk = Vector[(T, U)]((key -> value))
      val size = conf.chunkSizer(value)

      val newNode = new ChunkListNode(chunk, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val newTailMod = datastore.createMod[ChunkListNode[T, U]](null)
      val chunk = Vector[(T, U)]((key -> value))
      val newNode = new ChunkListNode(chunk, newTailMod, conf.chunkSizer(value))

      futures = datastore.updateMod(tailMod.id, newNode)

      lastNodeMod = tailMod
      tailMod = newTailMod
    } else {
      val chunk = lastNode.chunk :+ (key -> value)
      val size = lastNode.size + conf.chunkSizer(value)

      val newNode = new ChunkListNode(chunk, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    }

    futures
  }

  def update(key: T, value: U): ArrayBuffer[Future[String]] = {
    var node = datastore.getMod(list.head.id).asInstanceOf[ChunkListNode[T, U]]
    var previousNode: ChunkListNode[T, U] = null

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var oldValue: U = null.asInstanceOf[U]
    while (!found && node != null) {

      val newChunk = node.chunk.map{ case (_key, _value) => {
        if (!found && key == _key) {
          found = true
          oldValue = _value
          (_key -> value)
        } else {
          (_key -> _value)
        }
      }}

      if (found) {
        val newSize = node.size + conf.chunkSizer(value) - conf.chunkSizer(oldValue)
        val newNode = new ChunkListNode(newChunk, node.nextMod, newSize)

        if (previousNode != null) {
          futures = datastore.updateMod(previousNode.nextMod.id, newNode)
        } else {
          futures = datastore.updateMod(list.head.id, newNode)
        }
      } else {
        previousNode = node
        node = datastore.getMod(node.nextMod.id).asInstanceOf[ChunkListNode[T, U]]
      }
    }

    futures
  }

  def remove(key: T): ArrayBuffer[Future[String]] = {
    var previousNode: ChunkListNode[T, U] = null
    var previousMod: Mod[ChunkListNode[T, U]] = null
    var mod = list.head
    var node = datastore.getMod(list.head.id).asInstanceOf[ChunkListNode[T, U]]

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var oldValue: U = null.asInstanceOf[U]
    while (!found && node != null) {
      val newChunk = node.chunk.filter{ case (_key, _value) => {
        if (!found && key == _key) {
          oldValue = _value
          found = true
          false
        } else {
          true
        }
      }}

      if (found) {
        if (newChunk.size == 0) {
          val lastNode = datastore.getMod(lastNodeMod.id)
            .asInstanceOf[ChunkListNode[T, U]]
          val nextNode = datastore.getMod(node.nextMod.id)
            .asInstanceOf[ChunkListNode[T, U]]

          if (node == lastNode) {
            assert(nextNode == null)
            lastNodeMod = previousMod
          } else if (lastNodeMod.id == node.nextMod.id) {
            lastNodeMod = previousNode.nextMod
          }

          if (previousNode == null) {
            futures = datastore.updateMod(list.head.id, nextNode)
          } else {
            futures = datastore.updateMod(previousNode.nextMod.id,
                                          nextNode)
          }
        } else {
          val newSize = node.size - conf.chunkSizer(oldValue)
          val newNode = new ChunkListNode(newChunk, node.nextMod, newSize)

          if (previousNode == null) {
            futures = datastore.updateMod(list.head.id, newNode)
          } else {
            futures = datastore.updateMod(previousNode.nextMod.id, newNode)
          }
        }
      } else {
        previousNode = node
        previousMod = mod
        node = datastore.getMod(node.nextMod.id)
          .asInstanceOf[ChunkListNode[T, U]]
        mod = node.nextMod
      }
    }

    futures
  }

  def contains(key: T): Boolean = {
    var found = false
    var innerNode = datastore.getMod(list.head.id)
      .asInstanceOf[ChunkListNode[T, U]]

    while (innerNode != null && !found) {
      innerNode.chunk.map{ case (_key, _value) =>
	if (key == _key)
	  found = true
      }

      innerNode = datastore.getMod(innerNode.nextMod.id)
	.asInstanceOf[ChunkListNode[T, U]]
    }

    found
  }

  def getModifiable(): Any = list
}
