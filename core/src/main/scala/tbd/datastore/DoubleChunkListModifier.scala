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
import tbd.mod.{DoubleChunkList, DoubleChunkListNode, Mod}

class DoubleChunkListModifier[T, U](
    _datastore: Datastore,
    conf: ListConf) extends Modifier[T, U](_datastore) {
  private var tailMod: Mod[DoubleChunkListNode[T, U]] = null

  // Contains the last DoubleChunkListNode before the tail node.
  private var lastNodeMod: Mod[DoubleChunkListNode[T, U]] = null

  val list = initialize()

  private def initialize(): DoubleChunkList[T, U] = {
    var tail = datastore.createMod[DoubleChunkListNode[T, U]](null)
    tailMod = tail

    val lastChunkMod = datastore.createMod(Vector[(T, U)]())
    tail = datastore.createMod(new DoubleChunkListNode(lastChunkMod, tail, 0))
    lastNodeMod = tail

    var chunk = Vector[(T, U)]()
    var size = 0
    /*for (elem <- table) {
      if (size >= conf.chunkSize) {
	val chunkMod = datastore.createMod(chunk)
	tail = datastore.createMod(new DoubleChunkListNode(chunkMod, tail, size))

	chunk = Vector[(T, U)]()
	size = 0
      }

      chunk = chunk :+ (elem._1.asInstanceOf[T] -> elem._2.asInstanceOf[U])
      size += conf.chunkSizer(elem._2.asInstanceOf[U])
    }*/

    if (size > 0) {
      val chunkMod = datastore.createMod(chunk)
      tail = datastore.createMod(new DoubleChunkListNode(chunkMod, tail, size))
    }

    new DoubleChunkList[T, U](tail)
  }

  def insert(key: T, value: U): ArrayBuffer[Future[String]] = {
    val lastNode =
      datastore.getMod(lastNodeMod.id).asInstanceOf[DoubleChunkListNode[T, U]]

    var futures = ArrayBuffer[Future[String]]()
    if (lastNode == null) {
      val chunkMod = datastore.createMod(Vector[(T, U)]((key -> value)))
      val size = conf.chunkSizer(value)

      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val lastChunk = datastore.getMod(lastNode.chunkMod.id).asInstanceOf[Vector[(T, U)]]

      val newTailMod = datastore.createMod[DoubleChunkListNode[T, U]](null)
      val chunkMod = datastore.createMod(Vector[(T, U)]((key -> value)))
      val newNode = new DoubleChunkListNode(chunkMod, newTailMod, conf.chunkSizer(value))

      futures = datastore.updateMod(tailMod.id, newNode)

      lastNodeMod = tailMod
      tailMod = newTailMod
    } else {
      val lastChunk =
        datastore.getMod(lastNode.chunkMod.id).asInstanceOf[Vector[(T, U)]]
      val chunkMod = datastore.createMod(lastChunk :+ (key -> value))
      val size = lastNode.size + conf.chunkSizer(value)

      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    }

    futures
  }

  def update(key: T, value: U): ArrayBuffer[Future[String]] = {
    var node = datastore.getMod(list.head.id).asInstanceOf[DoubleChunkListNode[T, U]]

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    while (!found && node != null) {
      val chunk = datastore.getMod(node.chunkMod.id).asInstanceOf[Vector[(T, U)]]

      val newChunk = chunk.map{ case (_key, _value) => {
        if (!found && key == _key) {
          found = true
          (_key -> value)
        } else {
          (_key -> _value)
        }
      }}

      if (found) {
        futures = datastore.updateMod(node.chunkMod.id, newChunk)
      } else {
        node = datastore.getMod(node.nextMod.id).asInstanceOf[DoubleChunkListNode[T, U]]
      }
    }

    futures
  }

  def remove(key: T): ArrayBuffer[Future[String]] = {
    var previousNode: DoubleChunkListNode[T, U] = null
    var previousMod: Mod[DoubleChunkListNode[T, U]] = null
    var mod = list.head
    var node = datastore.getMod(list.head.id).asInstanceOf[DoubleChunkListNode[T, U]]

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    while (!found && node != null) {
      val chunk = datastore.getMod(node.chunkMod.id).asInstanceOf[Vector[(T, U)]]

      val newChunk = chunk.filter{ case (_key, _value) => {
        if (!found && key == _key) {
          found = true
          false
        } else {
          true
        }
      }}

      if (found) {
        if (newChunk.size == 0) {
          val lastNode = datastore.getMod(lastNodeMod.id)
          val nextNode = datastore.getMod(node.nextMod.id)
            .asInstanceOf[DoubleChunkListNode[T, U]]

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
          futures = datastore.updateMod(node.chunkMod.id, newChunk)
        }
      } else {
        previousNode = node
        previousMod = mod
        node = datastore.getMod(node.nextMod.id)
          .asInstanceOf[DoubleChunkListNode[T, U]]
        mod = node.nextMod
      }
    }

    futures
  }

  def contains(key: T): Boolean = {
    var found = false
    var innerNode = datastore.getMod(list.head.id)
      .asInstanceOf[DoubleChunkListNode[T, U]]

    while (innerNode != null && !found) {
      val chunk = datastore.getMod(innerNode.chunkMod.id)
	.asInstanceOf[Vector[(T, U)]]
      chunk.map{ case (_key, _value) =>
	if (key == _key)
	  found = true
      }

      innerNode = datastore.getMod(innerNode.nextMod.id)
	.asInstanceOf[DoubleChunkListNode[T, U]]
    }

    found
  }

  def getModifiable(): Any = list
}
