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
import java.io.Serializable
import scala.collection.mutable.Buffer

import tdb.{Context, Mod, Mutator}

class PartitionedAggregatorList[T]
    (partitions: Buffer[AggregatorList[T, Int]], conf: ListConf)
  extends AdjustableList[T, Int] with Serializable {

  def filter(pred: ((T, Int)) => Boolean)
      (implicit c: Context): AdjustableList[T, Int] = ???

  def flatMap[V, W](f: ((T, Int)) => Iterable[(V, W)])
      (implicit c: Context): AdjustableList[V, W] = ???

  def join[V]
      (that: AdjustableList[T, V], condition: ((T, V), (T, Int)) => Boolean)
      (implicit c: Context): AdjustableList[T, (Int, V)] = ???

  def map[V, W](f: ((T, Int)) => (V, W))
      (implicit c: Context): AdjustableList[V, W] = ???

  def reduce(f: ((T, Int), (T, Int)) => (T, Int))
      (implicit c: Context): Mod[(T, Int)] = ???

  def sortJoin[V](that: AdjustableList[T, V])
      (implicit c: Context,
       ordering: Ordering[T]): AdjustableList[T, (Int, V)] = ???

  def split(pred: ((T, Int)) => Boolean)
      (implicit c: Context):
  (AdjustableList[T, Int], AdjustableList[T, Int]) = ???

  /* Meta functions */
  def toBuffer(mutator: Mutator): Buffer[(T, Int)] = {
    val buf = Buffer[(T, Int)]()

    for (partition <- partitions) {
      buf ++= partition.toBuffer(mutator)
    }

    buf
  }
}
