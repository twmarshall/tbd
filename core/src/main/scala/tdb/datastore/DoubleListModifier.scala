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

class DoubleListModifier(datastore: Datastore) extends Modifier {
  import datastore.context.dispatcher
  private var tailMod = datastore.createMod[DoubleListNode[Any, Any]](null)

  val nodes = Map[Any, Mod[DoubleListNode[Any, Any]]]()

  val modList = new DoubleList[Any, Any](tailMod, false, datastore.workerId)

  def load(data: Map[Any, Any]): Future[_] = {
    var tail = datastore.createMod[DoubleListNode[Any, Any]](null)
    val newTail = tail

    var headNode: DoubleListNode[Any, Any] = null
    var headValue: (Any, Any) = null

    for ((key, value) <- data) {
      headValue = ((key, value))
      val valueMod = datastore.createMod(headValue)
      headNode = new DoubleListNode(valueMod, tail)
      tail = datastore.createMod(headNode)

      nodes(key) = tail
    }

    val future = datastore.asyncUpdate(tailMod, headNode)
    nodes(headValue._1) = tailMod
    tailMod = newTail

    future
  }

  def loadInput(keys: Iterable[String]): Future[_] = {
    var tail = datastore.createMod[DoubleListNode[Any, Any]](null)
    val newTail = tail

    var headNode: DoubleListNode[Any, Any] = null
    var headKey: Any = null.asInstanceOf[Any]

    for (key <- keys) {
      headKey = key.asInstanceOf[Any]
      val valueMod = new Mod[(Any, Any)](datastore.getNewModId())
      datastore.inputs(valueMod.id) = key
      headNode = new DoubleListNode(valueMod, tail)
      tail = datastore.createMod(headNode)

      nodes(headKey) = tail
    }

    val future = datastore.asyncUpdate(tailMod, headNode)
    nodes(headKey) = tailMod
    tailMod = newTail

    future
  }

  private def append(key: Any, value: Any): Future[_] = {
    val valueMod = datastore.createMod((key, value))
    val newTail = datastore.createMod[DoubleListNode[Any, Any]](null)
    val newNode = new DoubleListNode(valueMod, newTail)

    val future = datastore.asyncUpdate(tailMod, newNode)

    nodes(key) = tailMod

    tailMod = newTail

    future
  }

  def put(key: Any, value: Any): Future[_] = {
    if (!nodes.contains(key)) {
      append(key, value)
    } else {
      val nextMod = datastore.read(nodes(key)).nextMod
      val valueMod = datastore.createMod((key, value))
      val newNode = new DoubleListNode(valueMod, nextMod)

      datastore.asyncUpdate(nodes(key), newNode)
    }
  }

  def remove(key: Any, value: Any): Future[_] = {
    val mod = nodes(key)

    val node = datastore.read(mod)

    val nextNode = datastore.read(node.nextMod)

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = mod
    } else {
      val nextValue = datastore.read(nextNode.value)
      nodes(nextValue._1) = mod
    }

    val future = datastore.asyncUpdate(mod, nextNode)
    nodes -= key

    future
  }

  def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): DoubleList[Any, Any] = {
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
}
