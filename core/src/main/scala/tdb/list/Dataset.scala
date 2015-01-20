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

import scala.collection.mutable.Map

import tdb.Constants.WorkerId

trait Dataset[T, U] extends ListInput[T, U] {
  def numPartitions: Int = getPartitions.size

  def getPartitions: Iterable[Partition[T, U]]

  def getPartitionsByWorker(): Map[WorkerId, Iterable[Partition[T, U]]] = {
    val output = Map[WorkerId, Iterable[Partition[T, U]]]()

    for (partition <- getPartitions) {
      if (output.contains(partition.workerId)) {
        output(partition.workerId) =
          output(partition.workerId) ++ List(partition)
      } else {
        output(partition.workerId) = Iterable(partition)
      }
    }

    output
  }
}
