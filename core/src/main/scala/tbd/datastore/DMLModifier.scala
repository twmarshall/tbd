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

import tbd.mod._

class DMLModifier[T, U](
    aDatastore: Datastore,
    table: Map[Any, Any] = Map[Any, Any]()) extends Modifier[T, U](aDatastore) {
  val doubleModList = initialize()

  private def initialize(): DoubleModList[T, U] = {
    var tail = datastore.createMod[DoubleModListNode[T, U]](null)

    for (elem <- table) {
    //for (elem <- scala.collection.immutable.TreeMap(table.asInstanceOf[Map[Int, U]].toSeq: _*)) {
      //println("creating node " + elem._1)
      val newNode = new DoubleModListNode[T, U](
                  datastore.createMod(elem.asInstanceOf[(T, U)]), tail)
      tail = datastore.createMod(newNode)
    }

    new DoubleModList[T, U](tail)
  }

  def insert(key: T, value: U): ArrayBuffer[Future[String]] = {
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]
    var previousNode: DoubleModListNode[T, U] = null
    while (innerNode != null) {
      previousNode = innerNode
      innerNode = datastore.getMod(innerNode.next.id)
        .asInstanceOf[DoubleModListNode[T, U]]
    }

    val newTail = datastore.createMod[DoubleModListNode[T, U]](null)
    val newNode = new DoubleModListNode(datastore.createMod((key, value)), newTail)

    if (previousNode != null) {
      datastore.updateMod(previousNode.next.id, newNode)
    } else {
      datastore.updateMod(doubleModList.head.id, newNode)
    }
  }

  def update(key: T, value: U): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]

    while (innerNode != null && !found) {
      val oldValue = datastore.getMod(innerNode.value.id)
	.asInstanceOf[(T, U)]
      if (oldValue._1 == key) {
	futures = datastore.updateMod(innerNode.value.id, (key, value))
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def remove(key: T): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]

    var previousNode: DoubleModListNode[T, U] = null
    while (innerNode != null && !found) {
      val oldValue = datastore.getMod(innerNode.value.id)
	.asInstanceOf[(T, U)]
      if (oldValue._1 == key) {
        if (previousNode != null) {
          futures = datastore.updateMod(previousNode.next.id,
                                       datastore.tables("mods")(innerNode.next.id))
        } else {
          futures = datastore.updateMod(doubleModList.head.id,
                                       datastore.tables("mods")(innerNode.next.id))
        }

        // Note: for now, we're not addressing what happens if you try to get
        // a value from the table that doesn't exist, so we don't need to
        // notify workers when a mod is removed.
        datastore.tables("mods") -= innerNode.next.id
	datastore.tables("mods") -= innerNode.value.id

	found = true
      } else {
        previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    futures
  }

  def contains(key: T): Boolean = {
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T, U]]

    while (innerNode != null && !found) {
      val oldValue = datastore.getMod(innerNode.value.id)
	.asInstanceOf[(T, U)]
      if (oldValue._1 == key) {
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T, U]]
      }
    }

    found
  }

  def getModifiable(): AdjustableList[T, U] = {
    doubleModList
  }
}
