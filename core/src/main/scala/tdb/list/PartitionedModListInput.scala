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
package tdb.list

import scala.collection.mutable.{Buffer, Map}

class PartitionedModListInput[T, U]
    (partitions: Buffer[ModListInput[T, U]])
  extends Dataset[T, U] with java.io.Serializable {

  private def getPartition(key: T) =
    partitions(key.hashCode().abs % partitions.size)

  def put(key: T, value: U) = {
    getPartition(key).put(key, value)
  }

  def asyncPut(key: T, value: U) = {
    getPartition(key).asyncPut(key, value)
  }

  def update(key: T, value: U) = {
    getPartition(key).update(key, value)
  }

  def remove(key: T, value: U) = {
    getPartition(key).remove(key, value)
  }

  def load(data: Map[T, U]) = {
    for ((key, value) <- data) {
      getPartition(key).put(key, value)
    }
  }

  def getPartitions = partitions

  def getAdjustableList(): AdjustableList[T, U] = {
    val adjustablePartitions = Buffer[ModList[T, U]]()
    for (partition <- partitions) {
      adjustablePartitions += partition.getAdjustableList()
    }

    new PartitionedModList(adjustablePartitions)
  }
}
