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

class ListModifier[T, U](datastore: Datastore) extends ListInput[T, U] {
  private var tailMod = datastore.createMod[ModListNode[T, U]](null)

  val nodes = Map[T, Mod[ModListNode[T, U]]]()

  val modList = new ModList[T, U](tailMod, false, datastore.workerId)

  def load(data: Map[T, U]) {
    var tail = datastore.createMod[ModListNode[T, U]](null)
    val newTail = tail

    for ((key, value) <- data) {
      tail = datastore.createMod(new ModListNode((key, value), tail))
      nodes(key) = tail
    }

    val head = datastore.read(tail)
    datastore.update(tailMod, head)
    nodes(head.value._1) = tailMod
    tailMod = newTail
  }

  def put(key: T, value: U) {
    Await.result(asyncPut(key, value), DURATION)
  }

  def asyncPut(key: T, value: U): Future[_] = {
    val newTail = datastore.createMod[ModListNode[T, U]](null)
    val newNode = new ModListNode((key, value), newTail)

    val future = datastore.asyncUpdate(tailMod, newNode)

    nodes(key) = tailMod
    tailMod = newTail

    future
  }

  def update(key: T, value: U) {
    val nextMod = datastore.read(nodes(key)).nextMod
    val newNode = new ModListNode((key, value), nextMod)

    datastore.update(nodes(key), newNode)
  }

  def remove(key: T, value: U) {
    val node = datastore.read(nodes(key))
    val nextNode = datastore.read(node.nextMod)

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = nodes(key)
    } else {
      nodes(nextNode.value._1) = nodes(key)
    }

    datastore.update(nodes(key), nextNode)

    nodes -= key
  }

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): AdjustableList[T, U] = {
    modList
  }
}
