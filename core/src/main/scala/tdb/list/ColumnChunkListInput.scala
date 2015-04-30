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
import scala.collection.mutable
import scala.concurrent.Await

import ColumnList._
import tdb.Constants._
import tdb.messages._
import tdb.util.ObjHasher

class ColumnChunkListInput[T]
    (_inputId: InputId,
     _hasher: ObjHasher[TaskId],
     conf: ColumnListConf,
     masterRef: ActorRef)
  extends ColumnListInput[T](_inputId, _hasher, conf, masterRef) {
  import scala.concurrent.ExecutionContext.Implicits.global

  override def getAdjustableList(): AdjustableList[T, Columns] = {
    val adjustablePartitions = mutable.Buffer[ColumnChunkList[T]]()

    for (datastoreId <- hasher.objs.values) {
      val future = resolver.send(datastoreId, GetAdjustableListMessage())
      adjustablePartitions +=
        Await.result(future.mapTo[ColumnChunkList[T]], DURATION)
    }

    new PartitionedColumnChunkList(adjustablePartitions, conf)
  }

}
