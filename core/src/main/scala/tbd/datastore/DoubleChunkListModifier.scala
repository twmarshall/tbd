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

class DoubleChunkListModifier(
    _datastore: Datastore,
    conf: ListConf) extends Modifier(_datastore) {
  private var tailMod: Mod[DoubleChunkListNode[Any, Any]] = null

  // Contains the last DoubleChunkListNode before the tail node.
  private var lastNodeMod: Mod[DoubleChunkListNode[Any, Any]] = null

  val list = initialize()

  private def initialize(): DoubleChunkList[Any, Any] = {
    var tail = datastore.createMod[DoubleChunkListNode[Any, Any]](null)
    tailMod = tail

    val lastChunkMod = datastore.createMod(Vector[(Any, Any)]())
    tail = datastore.createMod(new DoubleChunkListNode(lastChunkMod, tail, 0))
    lastNodeMod = tail

    /*if (conf.file != "") {
      var chunk = Vector[(Any, Any)]()
      var size = 0

      val elems = scala.xml.XML.loadFile(conf.file)
      (elems \\ "elem").map(elem => {
	(elem \\ "key").map(key => {
	  (elem \\ "value").map(value => {
	    if (size >= conf.chunkSize) {
	      val chunkMod = datastore.createMod(chunk)
	      tail = datastore.createMod(new DoubleChunkListNode(chunkMod, tail, size))
	      chunk = Vector[(Any, Any)]()
	      size = 0
	    }

	    chunk = chunk :+ (key.text -> value.text).asInstanceOf[(Any, Any)]
	    size += conf.chunkSizer(value.text)
	  })
	})
      })

      if (size > 0) {
	val chunkMod = datastore.createMod(chunk)
	tail = datastore.createMod(new DoubleChunkListNode(chunkMod, tail, size))
      }
    }*/

    new DoubleChunkList[Any, Any](tail)
  }

  def insert(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    val lastNode =
      datastore.getMod(lastNodeMod.id).asInstanceOf[DoubleChunkListNode[Any, Any]]

    var futures = ArrayBuffer[Future[String]]()
    if (lastNode == null) {
      val chunkMod = datastore.createMod(Vector[(Any, Any)]((key -> value)))
      val size = conf.chunkSizer(value)

      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    } else if (lastNode.size >= conf.chunkSize) {
      val lastChunk = datastore.getMod(lastNode.chunkMod.id).asInstanceOf[Vector[(Any, Any)]]

      val newTailMod = datastore.createMod[DoubleChunkListNode[Any, Any]](null)
      val chunkMod = datastore.createMod(Vector[(Any, Any)]((key -> value)))
      val newNode = new DoubleChunkListNode(chunkMod, newTailMod, conf.chunkSizer(value))

      futures = datastore.updateMod(tailMod.id, newNode)

      lastNodeMod = tailMod
      tailMod = newTailMod
    } else {
      val lastChunk =
        datastore.getMod(lastNode.chunkMod.id).asInstanceOf[Vector[(Any, Any)]]
      val chunkMod = datastore.createMod(lastChunk :+ (key -> value))
      val size = lastNode.size + conf.chunkSizer(value)

      val newNode = new DoubleChunkListNode(chunkMod, tailMod, size)
      futures = datastore.updateMod(lastNodeMod.id, newNode)
    }

    futures
  }

  def update(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    var node = datastore.getMod(list.head.id).asInstanceOf[DoubleChunkListNode[Any, Any]]

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    while (!found && node != null) {
      val chunk = datastore.getMod(node.chunkMod.id).asInstanceOf[Vector[(Any, Any)]]

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
        node = datastore.getMod(node.nextMod.id).asInstanceOf[DoubleChunkListNode[Any, Any]]
      }
    }

    futures
  }

  def remove(key: Any): ArrayBuffer[Future[String]] = {
    var previousNode: DoubleChunkListNode[Any, Any] = null
    var previousMod: Mod[DoubleChunkListNode[Any, Any]] = null
    var mod = list.head
    var node = datastore.getMod(list.head.id).asInstanceOf[DoubleChunkListNode[Any, Any]]

    var futures = ArrayBuffer[Future[String]]()
    var found = false
    while (!found && node != null) {
      val chunk = datastore.getMod(node.chunkMod.id).asInstanceOf[Vector[(Any, Any)]]

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
            .asInstanceOf[DoubleChunkListNode[Any, Any]]

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
          .asInstanceOf[DoubleChunkListNode[Any, Any]]
        mod = node.nextMod
      }
    }

    futures
  }

  def contains(key: Any): Boolean = {
    var found = false
    var innerNode = datastore.getMod(list.head.id)
      .asInstanceOf[DoubleChunkListNode[Any, Any]]

    while (innerNode != null && !found) {
      val chunk = datastore.getMod(innerNode.chunkMod.id)
	.asInstanceOf[Vector[(Any, Any)]]
      chunk.map{ case (_key, _value) =>
	if (key == _key)
	  found = true
      }

      innerNode = datastore.getMod(innerNode.nextMod.id)
	.asInstanceOf[DoubleChunkListNode[Any, Any]]
    }

    found
  }

  def getModifiable(): Any = list
}
