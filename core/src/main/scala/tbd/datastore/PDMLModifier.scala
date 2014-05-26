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

class PDMLModifier[T, U](
    aDatastore: Datastore,
    table: Map[Any, Any],
    numPartitions: Int) extends Modifier[T, U](aDatastore) {

  val partitionModifiers = ArrayBuffer[DMLModifier[T, U]]()
  val modList = initialize()

  private def initialize(): PartitionedDoubleModList[T, U] = {
    val partitions = new ArrayBuffer[DoubleModList[T, U]]()
    var dmlModifier = new DMLModifier[T, U](datastore)

    val partitionSize = math.max(1, table.size / numPartitions)
    var i = 1
    for (elem <- table) {
      dmlModifier.insert(elem._1.asInstanceOf[T],
			 elem._2.asInstanceOf[U])

      if (i % partitionSize == 0) {
        partitionModifiers += dmlModifier
        partitions += dmlModifier.doubleModList
        dmlModifier = new DMLModifier[T, U](datastore)
      }
      i += 1
    }

    if ((i - 1) % partitionSize != 0) {
      partitionModifiers += dmlModifier
      partitions += dmlModifier.doubleModList
    }

    new PartitionedDoubleModList[T, U](partitions)
  }

  def insert(key: T, value: U): ArrayBuffer[Future[String]] = {
    partitionModifiers(0).insert(key, value)
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
