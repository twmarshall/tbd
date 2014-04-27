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

import tbd.mod.{DoubleModList, Mod, PartitionedDoubleModList}

class PDMLModifier[T](
    aDatastore: Datastore,
    table: Map[Any, Any],
    partitions: Int) extends Modifier[T](aDatastore) {

  val modList = initialize()

  private def initialize(): PartitionedDoubleModList[T] = {
    var tail = datastore.createMod[DoubleModList[T]](null)
    var outputTail = datastore.createMod[DoubleModList[Mod[DoubleModList[T]]]](null)
    val partitionSize = math.max(1, table.size / partitions)
    var i = 1
    for (elem <- table) {
      val head = datastore.createMod(new DoubleModList(elem._2.asInstanceOf[Mod[T]], tail))
      if (i % partitionSize == 0) {
        val headMod = datastore.createMod(head)
        outputTail = datastore.createMod(new DoubleModList[Mod[DoubleModList[T]]](headMod, outputTail))
        tail = datastore.createMod[DoubleModList[T]](null)
      } else {
        tail = head
      }
      i += 1
    }

    if ((i - 1) % partitionSize != 0) {
      val headMod = datastore.createMod(tail)
      outputTail = datastore.createMod(new DoubleModList(headMod, outputTail))
    }

    new PartitionedDoubleModList[T](outputTail)
  }

  def insert(mod: Mod[T], respondTo: ActorRef): Int = {
    var outerNode = datastore.getMod(modList.lists.id)
      .asInstanceOf[DoubleModList[Mod[DoubleModList[T]]]]

    var lastMod: Mod[DoubleModList[T]] = null
    while (outerNode != null) {
      val modList = datastore.getMod(outerNode.value.id)
        .asInstanceOf[Mod[DoubleModList[T]]]
      var innerNode = datastore.getMod(modList.id)
        .asInstanceOf[DoubleModList[T]]

      while (innerNode != null) {
        lastMod = innerNode.next
        innerNode = datastore.getMod(innerNode.next.id)
          .asInstanceOf[DoubleModList[T]]
      }
      outerNode = datastore.getMod(outerNode.next.id)
        .asInstanceOf[DoubleModList[Mod[DoubleModList[T]]]]
    }

    val newTail = datastore.createMod[DoubleModList[Any]](null)
    datastore.updateMod(lastMod.id, new DoubleModList(mod.asInstanceOf[Mod[Any]], newTail), respondTo)
  }

  def remove(toRemove: Mod[T], respondTo: ActorRef): Int = {
    var count = 0
    var outerNode = datastore.getMod(modList.lists.id)
      .asInstanceOf[DoubleModList[Mod[DoubleModList[Any]]]]

    var found = false
    while (outerNode != null && !found) {
      val modList = datastore.getMod(outerNode.value.id)
        .asInstanceOf[Mod[DoubleModList[Any]]]
      var innerNode = datastore.getMod(modList.id)
        .asInstanceOf[DoubleModList[Any]]

      var previousNode: DoubleModList[Any] = null
      while (innerNode != null && !found) {
        if (innerNode.value == toRemove) {
          if (previousNode != null) {
            count += datastore.updateMod(previousNode.next.id,
                                         datastore.tables("mods")(innerNode.next.id),
                                         respondTo)
          } else {
            count += datastore.updateMod(modList.id,
                                         datastore.tables("mods")(innerNode.next.id),
                                         respondTo)
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
            .asInstanceOf[DoubleModList[Any]]
        }
      }
      outerNode = datastore.getMod(outerNode.next.id)
        .asInstanceOf[DoubleModList[Mod[DoubleModList[Any]]]]
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    count
  }
}
