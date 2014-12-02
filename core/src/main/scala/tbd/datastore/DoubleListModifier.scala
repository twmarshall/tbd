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

import scala.collection.mutable.Map

import tbd.{Mod, Mutator}
import tbd.list._

class DoubleListModifier[T, U](datastore: Datastore) extends ListInput[T, U] {
  private var tailMod = datastore.createMod[DoubleListNode[T, U]](null)

  val nodes = Map[T, Mod[DoubleListNode[T, U]]]()

  val modList = new DoubleList[T, U](tailMod, false, datastore.workerId)

  def load(data: Map[T, U]) {
    var tail = datastore.createMod[DoubleListNode[T, U]](null)
    val newTail = tail

    var headNode: DoubleListNode[T, U] = null
    var headValue: (T, U) = null

    for ((key, value) <- data) {
      headValue = ((key, value))
      val valueMod = datastore.createMod(headValue)
      headNode = new DoubleListNode(valueMod, tail)
      tail = datastore.createMod(headNode)

      nodes(key) = tail
    }

    datastore.update(tailMod, headNode)
    nodes(headValue._1) = tailMod
    tailMod = newTail
  }

  def put(key: T, value: U) {
    val newTail = datastore.createMod[DoubleListNode[T, U]](null)
    val valueMod = datastore.createMod((key, value))
    val newNode = new DoubleListNode(valueMod, newTail)

    datastore.update(tailMod, newNode)

    nodes(key) = tailMod
    tailMod = newTail
  }

  def putAfter(key: T, pair: (T, U)) {
    val before = datastore.read(nodes(key))

    val valueMod = datastore.createMod(pair)
    val newNode = new DoubleListNode(valueMod, before.nextMod)
    val newNodeMod = datastore.createMod(newNode)

    val newBefore = new DoubleListNode(before.value, newNodeMod)
    datastore.update(nodes(key), newBefore)

    nodes(pair._1) = newNodeMod
  }

  def update(key: T, value: U) {
    val nextMod = datastore.read(nodes(key)).nextMod
    val valueMod = datastore.createMod((key, value))
    val newNode = new DoubleListNode(valueMod, nextMod)

    datastore.update(nodes(key), newNode)
  }

  def remove(key: T) {
    val node = datastore.read(nodes(key))
    val nextNode = datastore.read(node.nextMod)

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = nodes(key)
    } else {
      val value = datastore.read(nextNode.value)
      nodes(value._1) = nodes(key)
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
