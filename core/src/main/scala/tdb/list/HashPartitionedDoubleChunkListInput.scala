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

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.Buffer
import scala.concurrent.Await

import tdb.Constants._
import tdb.messages._
import tdb.util.ObjHasher

class HashPartitionedDoubleChunkListInput[T, U]
    (val inputId: InputId,
     val hasher: ObjHasher[TaskId],
     val conf: ListConf,
     val workers: Iterable[ActorRef],
     masterRef: ActorRef)
  extends HashPartitionedListInput[T, U](masterRef) with java.io.Serializable {
  import scala.concurrent.ExecutionContext.Implicits.global

  def getAdjustableList(): AdjustableList[T, U] = {
    val adjustablePartitions = Buffer[DoubleChunkList[T, U]]()

    for (datastoreId <- hasher.objs.values) {
      val future = resolver.send(datastoreId, GetAdjustableListMessage())
      adjustablePartitions +=
        Await.result(future.mapTo[DoubleChunkList[T, U]], DURATION)
    }

    new HashPartitionedDoubleChunkList(adjustablePartitions, conf)
  }
}
