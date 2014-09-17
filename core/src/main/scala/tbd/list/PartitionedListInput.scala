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
package tbd.list

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.datastore.Datastore

class PartitionedListInput[T, U](conf: ListConf) extends ListInput[T, U] {
  import scala.concurrent.ExecutionContext.Implicits.global

  val partitionModifiers = ArrayBuffer[ModListInput[T, U]]()
  val partitionedModList = initialize()

  private def initialize(): PartitionedModList[T, U] = {
    val partitions = new ArrayBuffer[ModList[T, U]]()
    for (i <- 1 to conf.partitions) {
      val modListModifier = new ModListInput[T, U]()
      partitionModifiers += modListModifier
      partitions += modListModifier.modList
    }

    new PartitionedModList[T, U](partitions)
  }

  private var putInto = 0
  override def put(key: T, value: U) {
    putInto = (putInto + 1) % conf.partitions
    partitionModifiers(putInto).put(key, value)
  }

  def putAfter(key: T, newPair: (T, U)) {
    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        partitionModifier.putAfter(key, newPair)
        found = true
      }
    }

    if (!found) {
      println("Warning: tried to putAfter nonexistant key " + key)
    }
  }

  override def update(key: T, value: U) {
    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        partitionModifier.update(key, value)
        found = true
      }
    }

   if (!found) {
     println("Warning: tried to update nonexistant key " + key)
   }
  }

  override def remove(key: T) {
    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        partitionModifier.remove(key)
        found = true
      }
    }
  }

  override def getAdjustableList(): AdjustableList[T, U] = {
    partitionedModList
  }
}
