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
import scala.concurrent.Future

import tbd.mod._

class PartitionedModListModifier[T, U](
    aDatastore: Datastore,
    table: Map[Any, Any],
    numPartitions: Int) extends Modifier[T, U](aDatastore) {

  val partitionModifiers = ArrayBuffer[ModListModifier[T, U]]()
  val modList = initialize()

  private def initialize(): PartitionedModList[T, U] = {
    val partitions = new ArrayBuffer[ModList[T, U]]()
    for (i <- 1 to numPartitions) {
      val modListModifier = new ModListModifier[T, U](datastore)
      partitionModifiers += modListModifier
      partitions += modListModifier.modList
    }

    var insertInto = 0
    for ((key, value) <- table) {
      insert(key.asInstanceOf[T], value.asInstanceOf[U])
    }

    new PartitionedModList[T, U](partitions)
  }

  private var insertInto = 0
  def insert(key: T, value: U): ArrayBuffer[Future[String]] = {
    insertInto = (insertInto + 1) % numPartitions
    partitionModifiers(insertInto).insert(key, value)
  }

  def update(key: T, value: U): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()

    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        futures = partitionModifier.update(key, value)
        found = true
      }
    }

   if (!found) {
     println("Warning: tried to update nonexistant key")
   }

    futures
  }

  def remove(key: T): ArrayBuffer[Future[String]] = {
    var futures = ArrayBuffer[Future[String]]()

    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        futures = partitionModifier.remove(key)
        found = true
      }
    }

    futures
  }

  def getModifiable(): AdjustableList[T, U] = {
    modList
  }
}
