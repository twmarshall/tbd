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

class PartitionedChunkListInput[T, U](conf: ListConf)
    extends ListInput[T, U] {

  val partitionModifiers = ArrayBuffer[ChunkListInput[T, U]]()
  val list = initialize()

  private def initialize(): PartitionedChunkList[T, U] = {
    val partitions = new ArrayBuffer[ChunkList[T, U]]()
    for (i <- 1 to conf.partitions) {
      val chunkListModifier = new ChunkListInput[T, U](conf)
      partitionModifiers += chunkListModifier
      partitions += chunkListModifier.list
    }

    new PartitionedChunkList[T, U](partitions)
  }

  private var putInto = 0
  def put(key: T, value: U) {
    putInto = (putInto + 1) % conf.partitions
    partitionModifiers(putInto).put(key, value)
  }

  def update(key: T, value: U) {
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

  def remove(key: T) {
    var found = false
    for (partitionModifier <- partitionModifiers) {
      if (!found && partitionModifier.contains(key)) {
        partitionModifier.remove(key)
        found = true
      }
    }

   if (!found) {
     println("Warning: tried to remove nonexistant key " + key)
   }
  }

  def getAdjustableList() = list
}
