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

import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.{Future, Promise}

import tbd.ListConf
import tbd.mod._

class DoubleModListModifier(
    _datastore: Datastore,
    conf: ListConf) extends Modifier(_datastore) {
  val doubleModList =
    new DoubleModList[Any, Any](datastore.createMod[DoubleModListNode[Any, Any]](null))

  def insert(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[Any, Any]]
    var previousNode: DoubleModListNode[Any, Any] = null
    while (innerNode != null) {
      previousNode = innerNode
      innerNode = datastore.getMod(innerNode.next.id)
        .asInstanceOf[DoubleModListNode[Any, Any]]
    }

    val newTail = datastore.createMod[DoubleModListNode[Any, Any]](null)
    val newNode = new DoubleModListNode(datastore.createMod((key, value)), newTail)

    if (previousNode != null) {
      datastore.updateMod(previousNode.next.id, newNode)
    } else {
      datastore.updateMod(doubleModList.head.id, newNode)
    }
  }

  def update(key: Any, value: Any): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[Any, Any]]

    while (innerNode != null && !found) {
      val oldValue = datastore.getMod(innerNode.value.id)
	.asInstanceOf[(Any, Any)]
      if (oldValue._1 == key) {
	futures = datastore.updateMod(innerNode.value.id, (key, value))
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[Any, Any]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def remove(key: Any): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[Any, Any]]

    var previousNode: DoubleModListNode[Any, Any] = null
    while (innerNode != null && !found) {
      val oldValue = datastore.getMod(innerNode.value.id)
	.asInstanceOf[(Any, Any)]
      if (oldValue._1 == key) {
        if (previousNode != null) {
          futures = datastore.updateMod(previousNode.next.id,
                                        datastore.getMod(innerNode.next.id))
        } else {
          futures = datastore.updateMod(doubleModList.head.id,
                                        datastore.getMod(innerNode.next.id))
        }

        datastore.removeMod(innerNode.next.id)
        datastore.removeMod(innerNode.value.id)

	found = true
      } else {
        previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[Any, Any]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def contains(key: Any): Boolean = {
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[Any, Any]]

    while (innerNode != null && !found) {
      val oldValue = datastore.getMod(innerNode.value.id)
	.asInstanceOf[(Any, Any)]
      if (oldValue._1 == key) {
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[Any, Any]]
      }
    }

    found
  }

  def getModifiable(): AdjustableList[Any, Any] = {
    doubleModList
  }
}
