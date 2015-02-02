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

import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.Future

import tdb.{Mod, Mutator}
import tdb.Constants._
import tdb.list._

class DoubleListModifier[T, U](datastore: Datastore) extends Modifier[T, U] {
  import datastore.context.dispatcher
  private var tailMod = datastore.createMod[DoubleListNode[T, U]](null)

  val nodes = Map[T, Buffer[Mod[DoubleListNode[T, U]]]]()

  val modList = new DoubleList[T, U](tailMod, false, datastore.workerId)

  def load(data: Map[T, U]): Future[_] = {
    var tail = datastore.createMod[DoubleListNode[T, U]](null)
    val newTail = tail

    var headNode: DoubleListNode[T, U] = null
    var headValue: (T, U) = null

    for ((key, value) <- data) {
      headValue = ((key, value))
      val valueMod = datastore.createMod(headValue)
      headNode = new DoubleListNode(valueMod, tail)
      tail = datastore.createMod(headNode)

      nodes(key) = Buffer(tail)
    }

    val future = datastore.asyncUpdate(tailMod, headNode)
    nodes(headValue._1) = Buffer(tailMod)
    tailMod = newTail

    future
  }

  def loadInput(keys: Iterable[Int]): Future[_] = {
    var tail = datastore.createMod[DoubleListNode[T, U]](null)
    val newTail = tail

    var headNode: DoubleListNode[T, U] = null
    var headKey: T = null.asInstanceOf[T]

    for (key <- keys) {
      headKey = key.asInstanceOf[T]
      val valueMod = new Mod[(T, U)](datastore.getNewModId())
      datastore.inputs(valueMod.id) = key
      headNode = new DoubleListNode(valueMod, tail)
      tail = datastore.createMod(headNode)

      nodes(headKey) = Buffer(tail)
    }

    val future = datastore.asyncUpdate(tailMod, headNode)
    nodes(headKey) = Buffer(tailMod)
    tailMod = newTail

    future
  }

  def asyncPut(key: T, value: U): Future[_] = {
    val valueMod = datastore.createMod((key, value))
    putMod(key, valueMod)
  }

  def putMod(key: T, valueMod: Mod[(T, U)]) = {
    val newTail = datastore.createMod[DoubleListNode[T, U]](null)
    val newNode = new DoubleListNode(valueMod, newTail)

    val future = datastore.asyncUpdate(tailMod, newNode)

    if (nodes.contains(key)) {
      nodes(key) += tailMod
    } else {
      nodes(key) = Buffer(tailMod)
    }

    tailMod = newTail

    future
  }

  def update(key: T, value: U): Future[_] = {
    if (nodes(key).size > 1) {
      println("?????" + key)
    }
    val nextMod = datastore.read(nodes(key).head).nextMod
    val valueMod = datastore.createMod((key, value))
    val newNode = new DoubleListNode(valueMod, nextMod)

    datastore.asyncUpdate(nodes(key).head, newNode)
  }

  def remove(key: T, value: U): Future[_] = {
    val beforeSize = size()
    var node: DoubleListNode[T, U] = null
    var mod: Mod[DoubleListNode[T, U]] = null

    var found = false
    for (_mod <- nodes(key); if !found) {
      val _node = datastore.read(_mod)
      val _value = datastore.read(_node.value)
      if (_value._2 == value) {
        node = _node
        mod = _mod
        found = true
      }
    }

    val nextNode = datastore.read(node.nextMod)

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = mod
    } else {
      val nextValue = datastore.read(nextNode.value)
      nodes(nextValue._1) += mod
      nodes(nextValue._1) -= node.nextMod
    }

    val future = datastore.asyncUpdate(mod, nextNode)
    nodes(key) -= mod

    assert(size() == (beforeSize - 1))

    future
  }

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): DoubleList[T, U] = {
    modList
  }

  override def toString(): String = {
    val buf = new StringBuffer()

    var node = datastore.read(modList.head)
    while (node != null) {
      val value = datastore.read(node.value)
      buf.append(value + ", " + node.nextMod + " = ")
      node = datastore.read(node.nextMod)
    }

    buf.toString()
  }

  def size(): Int = {
    var size = 0

    var node = datastore.read(modList.head)
    while (node != null) {
      size += 1
      node = datastore.read(node.nextMod)
    }

    size
  }
}
