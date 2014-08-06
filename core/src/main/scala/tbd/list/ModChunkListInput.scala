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
package tbd.list

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.Map
import scala.concurrent.{Await, Future}

import tbd.Mod
import tbd.Constants._
import tbd.datastore.Datastore

class ModChunkListInput[T, U](conf: ListConf) extends ChunkListInput[T, U] {
  import scala.concurrent.ExecutionContext.Implicits.global

  protected var tailMod = Datastore.createMod[ChunkListNode[T, U]](null)

  // Contains the last ChunkListNode before the tail node. If the list is empty,
  // the contents of this mod will be null.
  protected var lastNodeMod = Datastore.createMod[ChunkListNode[T, U]](null)

  val nodes = Map[T, Mod[ChunkListNode[T, U]]]()
  val previous = Map[T, Mod[ChunkListNode[T, U]]]()

  val list = new ChunkList[T, U](lastNodeMod)

  def put(key: T, value: U) {
    val lastNode = Datastore.getMod(lastNodeMod.id)
      .asInstanceOf[ChunkListNode[T, U]]

    val newNode =
      if (lastNode == null) {
        val chunk = Vector[(T, U)]((key -> value))
        val size = conf.chunkSizer(value)

        previous(key) = null

        new ChunkListNode(chunk, tailMod, size)
      } else if (lastNode.size >= conf.chunkSize) {
        val newTailMod = Datastore.createMod[ChunkListNode[T, U]](null)
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

    val futures = Datastore.updateMod(lastNodeMod.id, newNode)
    Await.result(Future.sequence(futures), DURATION)
  } //ensuring(isValid())

  def update(key: T, value: U) {
    val node = Datastore.getMod(nodes(key).id).asInstanceOf[ChunkListNode[T, U]]

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

    val futures = Datastore.updateMod(nodes(key).id, newNode)
    Await.result(Future.sequence(futures), DURATION)
  } //ensuring(isValid())

  def remove(key: T) {
    val node = Datastore.getMod(nodes(key).id)
      .asInstanceOf[ChunkListNode[T, U]]

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
        val nextNode = Datastore.getMod(node.nextMod.id)
          .asInstanceOf[ChunkListNode[T, U]]

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

          val nextNextNode = Datastore.getMod(nextNode.nextMod.id)
            .asInstanceOf[ChunkListNode[T, U]]
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

    val futures = Datastore.updateMod(nodes(key).id, newNode)
    Await.result(Future.sequence(futures), DURATION)
  } //ensuring(isValid())

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  private def isValid(): Boolean = {
    var previousMod: Mod[ChunkListNode[T, U]] = null
    var previousNode: ChunkListNode[T, U] = null
    var mod = list.head
    var node = Datastore.getMod(mod.id)
      .asInstanceOf[ChunkListNode[T, U]]

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
      node = Datastore.getMod(mod.id)
        .asInstanceOf[ChunkListNode[T, U]]
    }

    valid
  }

  def getChunkList(): AdjustableChunkList[T, U] = list
}
