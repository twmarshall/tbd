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
import scala.concurrent.{Await, ExecutionContext, Future}

import tdb.{Mod, Mutator}
import tdb.Constants._
import tdb.list._

class DoubleListModifier
    (datastore: Datastore, datastoreId: TaskId, store: KVStore, recovery: Boolean)
    (implicit ec: ExecutionContext)
  extends Modifier {

  private var tailMod: Mod[DoubleListNode[Any, Any]] = null

  val nodes = Map[Any, Mod[DoubleListNode[Any, Any]]]()

  val modList =
    if (!recovery) {
      tailMod = datastore.createMod[DoubleListNode[Any, Any]](null)

      val metaId = store.createTable("meta", "String", "Long", null, false)
      store.put(metaId, datastoreId + "-head", tailMod.id)

      new DoubleList[Any, Any](tailMod, false, datastoreId)
    } else {
      val metaId = store.createTable("meta", "String", "Long", null, false)
      val headId = Await.result(
        store.get(metaId, datastoreId + "-head").mapTo[Long], DURATION)

      val headMod = new Mod[DoubleListNode[Any, Any]](headId)
      var nodeMod = headMod

      while (nodeMod != null) {
        val node = datastore.read(nodeMod)

        if (node == null) {
          nodeMod = null
        } else {
          val value = datastore.read(node.valueMod)
          nodes(value._1) = nodeMod

          nodeMod = node.nextMod
          tailMod = node.nextMod
        }
      }

      new DoubleList[Any, Any](new Mod(headId), false, datastoreId)
    }

  def loadInput(keys: Iterable[Any]): Future[_] = {
    var headNode = datastore.read(modList.head)
    var headKey =
      if (headNode == null)
        null
      else
        datastore.read(headNode.valueMod)._1

    var tail = datastore.createMod(headNode)

    if (headNode == null) {
      tailMod = tail
    }

    for (key <- keys) {
      headKey = key.asInstanceOf[Any]

      val valueMod = new Mod[(Any, Any)](datastore.getNewModId())
      datastore.inputs(valueMod.id) = key
      headNode = new DoubleListNode(valueMod, tail)
      tail = datastore.createMod(headNode)

      nodes(headKey) = tail
    }

    val future = datastore.updateMod(modList.head.id, headNode)
    nodes(headKey) = modList.head

    future
  }

  private def append(key: Any, value: Any): Future[_] = {
    val valueMod = datastore.createMod((key, value))
    val newTail = datastore.createMod[DoubleListNode[Any, Any]](null)
    val newNode = new DoubleListNode(valueMod, newTail)

    val future = datastore.updateMod(tailMod.id, newNode)

    nodes(key) = tailMod

    tailMod = newTail

    future
  }

  def put(key: Any, value: Any): Future[_] = {
    if (!nodes.contains(key)) {
      append(key, value)
    } else {
      val node = datastore.read(nodes(key))
      val newValueMod = datastore.createMod((key, value))
      val newNode = new DoubleListNode(newValueMod, node.nextMod)

      datastore.updateMod(nodes(key).id, newNode)
    }
  }

  def putIn(column: String, key: Any, value: Any): Future[_] = ???

  def get(key: Any): Any = ???

  def remove(key: Any, value: Any): Future[_] = {
    val mod = nodes(key)

    val node = datastore.read(mod)

    val nextNode = datastore.read(node.nextMod)

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = mod
    } else {
      val nextValue = datastore.read(nextNode.valueMod)
      nodes(nextValue._1) = mod
    }

    val future = datastore.updateMod(mod.id, nextNode)
    nodes -= key

    future
  }

  def contains(key: Any): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): DoubleList[Any, Any] = {
    modList
  }

  def toBuffer(): Buffer[(Any, Any)] = ???

  override def toString(): String = {
    val buf = new StringBuffer()

    var node = datastore.read(modList.head)
    while (node != null) {
      val value = datastore.read(node.valueMod)
      buf.append(value + ", " + node.nextMod + " = ")
      node = datastore.read(node.nextMod)
    }

    buf.toString()
  }
}
