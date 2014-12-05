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

import scala.collection.mutable.{Buffer, Map}

class PartitionedModListInput[T, U]
    (partitions: Buffer[ModListInput[T, U]])
  extends ListInput[T, U] with java.io.Serializable {

  def put(key: T, value: U) = {
    partitions(key.hashCode() % partitions.size).put(key, value)
  }

  def update(key: T, value: U) = {
    partitions(key.hashCode() % partitions.size).update(key, value)
  }

  def remove(key: T, value: U) = {
    partitions(key.hashCode() % partitions.size).remove(key, value)
  }

  def load(data: Map[T, U]) = {
    for ((key, value) <- data) {
      partitions(key.hashCode() % partitions.size).put(key, value)
    }
  }

  def putAfter(key: T, newPair: (T, U)) = ???

  def getAdjustableList(): AdjustableList[T, U] = {
    val adjustablePartitions = Buffer[ModList[T, U]]()
    for (partition <- partitions) {
      adjustablePartitions += partition.getAdjustableList()
    }

    new PartitionedModList(adjustablePartitions)
  }
}
