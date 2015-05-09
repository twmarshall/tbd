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

import akka.pattern.ask
import java.io.Serializable
import scala.collection.mutable.Buffer
import scala.concurrent.Await

import ColumnList._
import tdb._
import tdb.Constants._
import tdb.messages._
import tdb.TDB._

class PartitionedColumnChunkList[T]
    (val partitions: Buffer[ColumnChunkList[T]],
     conf: ListConf)
  extends AdjustableList[T, Columns] with Serializable {

  Log.debug("new PartitionedColumnChunkList")

  def filter(pred: ((T, Columns)) => Boolean)
      (implicit c: Context): PartitionedDoubleChunkList[T, Columns] = ???

  def flatMap[V, W](f: ((T, Columns)) => Iterable[(V, W)])
      (implicit c: Context): PartitionedDoubleChunkList[V, W] = ???

  def join[V](that: AdjustableList[T, V], condition: ((T, V), (T, Columns)) => Boolean)
      (implicit c: Context): PartitionedDoubleChunkList[T, (Columns, V)] = ???

  def map[V, W](f: ((T, Columns)) => (V, W))
      (implicit c: Context): PartitionedDoubleChunkList[V, W] = ???

  /*override def projection2
      (column1: String, column2: String, f: (T, Any, Any, Context) => Unit)
      (implicit c: Context): Unit = {
    def innerProjection2(i: Int)
        (implicit c: Context): Unit = {
      if (i < partitions.size) {
        parWithHint({
          c => partitions(i).projection2(column1, column2, f)(c)
        }, partitions(i).workerId)({
          c => innerProjection2(i + 1)(c)
        })
      }
    }

    innerProjection2(0)
  }*/

  override def projection2Chunk
      (column1: String,
       column2: String,
       f: (Iterable[T], Iterable[Any], Iterable[Any], Context) => Unit)
      (implicit c: Context): Unit = {
    def innerProjection2Chunk(i: Int)
        (implicit c: Context): Unit = {
      if (i < partitions.size) {
        parWithHint({
          c => partitions(i).projection2Chunk(column1, column2, f)(c)
        }, partitions(i).workerId)({
          c => innerProjection2Chunk(i + 1)(c)
        })
      }
    }

    innerProjection2Chunk(0)
  }

  def reduce(f: ((T, Columns), (T, Columns)) => (T, Columns))
      (implicit c: Context): Mod[(T, Columns)] = ???

  def split(pred: ((T, Columns)) => Boolean)
      (implicit c: Context):
    (PartitionedDoubleChunkList[T, Columns], PartitionedDoubleChunkList[T, Columns]) = ???

  /* Meta Operations */
  def toBuffer(mutator: Mutator): Buffer[(T, Columns)] = {
    val buf = Buffer[(T, Columns)]()

    for (partition <- partitions) {
      buf ++= partition.toBuffer(mutator)
    }

    buf
  }

  def toString(mutator: Mutator): String = {
    val buf = new StringBuffer()
    for (partition <- partitions) {
      buf.append(partition.toBuffer(mutator).toString)
    }
    buf.toString()
  }
}
