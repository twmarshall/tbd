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

import tbd.Constants.ModId
import tbd.ListConf
import tbd.mod.{ChunkList, ChunkListNode, Mod}

class ChunkListModifier(
    _datastore: Datastore,
    conf: ListConf) extends Modifier(_datastore) {
  protected var tailMod = datastore.createMod[ChunkListNode[Any, Any]](null)

  // Contains the last ChunkListNode before the tail node. If the list is empty,
  // the contents of this mod will be null.
  protected var lastNodeMod = datastore.createMod[ChunkListNode[Any, Any]](null)

  val nodes = Map[Any, Mod[ChunkListNode[Any, Any]]]()
  val previous = Map[Any, Mod[ChunkListNode[Any, Any]]]()

  val list = new ChunkList[Any, Any](lastNodeMod)

  def insert(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    val lastNode = datastore.getMod(lastNodeMod.id)
      .asInstanceOf[ChunkListNode[Any, Any]]

    val newNode =
      if (lastNode == null) {
        val chunk = Vector[(Any, Any)]((key -> value))
        val size = conf.chunkSizer(value)

        previous(key) = null

        new ChunkListNode(chunk, tailMod, size)
      } else if (lastNode.size >= conf.chunkSize) {
        val newTailMod = datastore.createMod[ChunkListNode[Any, Any]](null)
        val chunk = Vector[(Any, Any)]((key -> value))
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

    datastore.updateMod(lastNodeMod.id, newNode)
  } //ensuring(isValid())

  def update(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    val node = datastore.getMod(nodes(key).id).asInstanceOf[ChunkListNode[Any, Any]]

    var oldValue: Any = null.asInstanceOf[Any]
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

    datastore.updateMod(nodes(key).id, newNode)
  } //ensuring(isValid())

  def remove(key: Any): ArrayBuffer[Future[String]] = {
    val node = datastore.getMod(nodes(key).id)
      .asInstanceOf[ChunkListNode[Any, Any]]

    var oldValue: Any = null.asInstanceOf[Any]
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
        val nextNode = datastore.getMod(node.nextMod.id)
          .asInstanceOf[ChunkListNode[Any, Any]]

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

          val nextNextNode = datastore.getMod(nextNode.nextMod.id)
            .asInstanceOf[ChunkListNode[Any, Any]]
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

    datastore.updateMod(nodes(key).id, newNode)
  } //ensuring(isValid())

  def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  def isValid(): Boolean = {
    var previousMod: Mod[ChunkListNode[Any, Any]] = null
    var previousNode: ChunkListNode[Any, Any] = null
    var mod = list.head
    var node = datastore.getMod(mod.id)
      .asInstanceOf[ChunkListNode[Any, Any]]

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
      node = datastore.getMod(mod.id)
        .asInstanceOf[ChunkListNode[Any, Any]]
    }

    valid
  }

  def getModifiable(): Any = list
}
