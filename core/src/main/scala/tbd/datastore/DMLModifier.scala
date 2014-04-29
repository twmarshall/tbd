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
import scala.collection.mutable.Map

import tbd.mod.{DoubleModList, DoubleModListNode, Mod, ModId}

class DMLModifier[T](
    aDatastore: Datastore,
    table: Map[Any, Any] = Map[Any, Any]()) extends Modifier[T](aDatastore) {
  val doubleModList = initialize()
  modList = doubleModList

  private def initialize(): DoubleModList[T] = {
    var tail = datastore.createMod[DoubleModListNode[T]](null)

    for (elem <- table) {
      val newNode = new DoubleModListNode[T](elem._2.asInstanceOf[Mod[T]], tail)
      tail = datastore.createMod(newNode)
    }

    new DoubleModList[T](tail)
  }

  def insert(mod: Mod[T], respondTo: ActorRef): Int = {
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T]]
    var previousNode: DoubleModListNode[T] = null
    while (innerNode != null) {
      previousNode = innerNode
      innerNode = datastore.getMod(innerNode.next.id)
        .asInstanceOf[DoubleModListNode[T]]
    }

    val newTail = datastore.createMod[DoubleModListNode[T]](null)
    val newNode = new DoubleModListNode(mod, newTail)

    if (previousNode != null) {
      datastore.updateMod(previousNode.next.id, newNode, respondTo)
    } else {
      datastore.updateMod(doubleModList.head.id, newNode, respondTo)
    }
  }

  def remove(toRemove: Mod[T], respondTo: ActorRef): Int = {
    var count = 0
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T]]

    var previousNode: DoubleModListNode[T] = null
    while (innerNode != null && !found) {
      if (innerNode.valueMod == toRemove) {
        if (previousNode != null) {
          count += datastore.updateMod(previousNode.next.id,
                                       datastore.tables("mods")(innerNode.next.id),
                                       respondTo)
        } else {
          count += datastore.updateMod(doubleModList.head.id,
                                       datastore.tables("mods")(innerNode.next.id),
                                       respondTo)
        }

        // Note: for now, we're not addressing what happens if you try to get
        // a value from the table that doesn't exist, so we don't need to
        // notify workers when a mod is removed.
        datastore.tables("mods") -= innerNode.next.id
	datastore.tables("mods") -= innerNode.valueMod.id

	found = true
      } else {
        previousNode = innerNode
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    count
  }

  def contains(toRemove: Mod[T]): Boolean = {
    var found = false
    var innerNode = datastore.getMod(doubleModList.head.id)
      .asInstanceOf[DoubleModListNode[T]]

    while (innerNode != null && !found) {
      if (innerNode.valueMod == toRemove) {
	found = true
      } else {
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModListNode[T]]
      }
    }

    found
  }
}
