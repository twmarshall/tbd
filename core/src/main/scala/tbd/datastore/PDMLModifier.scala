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

import tbd.mod.{DoubleModList, DoubleModListNode, Mod, PartitionedDoubleModList}

class PDMLModifier[T](
    aDatastore: Datastore,
    table: Map[Any, Any],
    numPartitions: Int) extends Modifier[T](aDatastore) {

  val partitionModifiers = ArrayBuffer[DMLModifier[T]]()
  modList = initialize()

  private def initialize(): PartitionedDoubleModList[T] = {
    val partitions = new ArrayBuffer[DoubleModList[T]]()
    var dmlModifier = new DMLModifier[T](datastore)

    val partitionSize = math.max(1, table.size / numPartitions)
    var i = 1
    for (elem <- table) {
      dmlModifier.insert(elem._2.asInstanceOf[Mod[T]], null)

      if (i % partitionSize == 0) {
        partitionModifiers += dmlModifier
        partitions += dmlModifier.doubleModList
        dmlModifier = new DMLModifier[T](datastore)
      }
      i += 1
    }

    if ((i - 1) % partitionSize != 0) {
      partitionModifiers += dmlModifier
      partitions += dmlModifier.doubleModList
    }

    new PartitionedDoubleModList[T](partitions)
  }

  def insert(mod: Mod[T], respondTo: ActorRef): Int = {
    partitionModifiers(0).insert(mod, respondTo)
  }

  def remove(toRemove: Mod[T], respondTo: ActorRef): Int = {
    var count = 0

    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(toRemove)) {
        count = partitionModifier.remove(toRemove, respondTo)
        found = true
      }
    }

    count
  }
}
