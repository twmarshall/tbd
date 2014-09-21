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

import scala.collection.mutable.Map

import tbd.{Mod, Mutator}

class ChunkListInput[T, U](mutator: Mutator, conf: ListConf)
    extends ListInput[T, U] {
  protected var tailMod = mutator.createMod[ChunkListNode[T, U]](null)

  // Contains the last ChunkListNode before the tail node. If the list is empty,
  // the contents of this mod will be null.
  protected var lastNodeMod = mutator.createMod[ChunkListNode[T, U]](null)

  val nodes = Map[T, Mod[ChunkListNode[T, U]]]()
  val previous = Map[T, Mod[ChunkListNode[T, U]]]()

  val list = new ChunkList[T, U](lastNodeMod, conf)

  def put(key: T, value: U) {
    val lastNode = lastNodeMod.read()

    val newNode =
      if (lastNode == null) {
        val chunk = Vector[(T, U)]((key -> value))
        val size = conf.chunkSizer(value)

        previous(key) = null

        new ChunkListNode(chunk, tailMod, size)
      } else if (lastNode.size >= conf.chunkSize) {
        val newTailMod = mutator.createMod[ChunkListNode[T, U]](null)
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

    mutator.updateMod(lastNodeMod, newNode)
  } //ensuring(isValid())

  private def calculateSize(chunk: Vector[(T, U)]) = {
    chunk.aggregate(0)((sum: Int, pair: (T, U)) => sum + conf.chunkSizer(pair), _ + _)
  }

  def putAfter(key: T, newPair: (T, U)) {
    val insertIntoMod = nodes(key)
    val insertInto = nodes(key).read()
    val newSize = insertInto.size + conf.chunkSizer(newPair._2)

    var found = false
    val newChunk = insertInto.chunk.flatMap((pair: (T, U)) => {
      if (pair._1 == key) {
        found = true
        Vector(pair, newPair)
      } else {
        Vector(pair)
      }
    })

    val newNode =
      if (newSize > conf.chunkSize) {
        val (newChunk1, newChunk2) = newChunk.splitAt(insertInto.chunk.size / 2)

        val size1 = calculateSize(newChunk1)
        val size2 = calculateSize(newChunk2)

        val newNode2 = new ChunkListNode(newChunk2, insertInto.nextMod, size2)
        val newNodeMod2 = mutator.createMod(newNode2)

        newChunk2.foreach {
          case (_key, _value) =>
            nodes(_key) = newNodeMod2
            previous(_key) = insertIntoMod
        }

        if (newChunk1.contains(newPair)) {
          nodes(newPair._1) = insertIntoMod
          previous(newPair._1) = previous(insertInto.chunk.head._1)
        }

        new ChunkListNode(newChunk1, newNodeMod2, size1)
      } else {
        nodes(newPair._1) = insertIntoMod
        previous(newPair._1) = previous(insertInto.chunk.head._1)

        new ChunkListNode(
          newChunk,
          insertInto.nextMod,
          insertInto.size + conf.chunkSizer(newPair))
      }

    mutator.updateMod(insertIntoMod, newNode)
  }

  def update(key: T, value: U) {
    val node = nodes(key).read()

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

    mutator.updateMod(nodes(key), newNode)
  } //ensuring(isValid())

  def remove(key: T) {
    val node = nodes(key).read()

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
        val nextNode = node.nextMod.read()

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

          val nextNextNode = nextNode.nextMod.read()
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

    mutator.updateMod(nodes(key), newNode)
    nodes -= key
  } //ensuring(isValid())

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  private def isValid(): Boolean = {
    var previousMod: Mod[ChunkListNode[T, U]] = null
    var previousNode: ChunkListNode[T, U] = null
    var mod = list.head
    var node = mod.read()

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
      node = mod.read()
    }

    valid
  }

  def getAdjustableList() = list
}
