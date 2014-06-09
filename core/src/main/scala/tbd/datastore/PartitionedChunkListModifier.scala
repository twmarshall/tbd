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

class PartitionedChunkListModifier[T, U](
    _datastore: Datastore,
    table: Map[Any, Any],
    numPartitions: Int,
    chunkSize: Int,
    chunkSizer: U => Int) extends Modifier[T, U](_datastore) {

  val partitionModifiers = ArrayBuffer[ChunkListModifier[T, U]]()
  val list = initialize()

  private def initialize(): PartitionedChunkList[T, U] = {
    val partitions = new ArrayBuffer[ChunkList[T, U]]()
    for (i <- 1 to numPartitions) {
      val chunkListModifier = new ChunkListModifier[T, U](datastore, Map[Any, Any](), chunkSize, chunkSizer)
      partitionModifiers += chunkListModifier
      partitions += chunkListModifier.list
    }

    var insertInto = 0
    for ((key, value) <- table) {
      insert(key.asInstanceOf[T], value.asInstanceOf[U])
    }

    new PartitionedChunkList[T, U](partitions)
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
     println("Warning: tried to update nonexistant key " + key)
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

   if (!found) {
     println("Warning: tried to remove nonexistant key " + key)
   }

    futures
  }

  def getModifiable(): AdjustableList[T, U] = list
}

