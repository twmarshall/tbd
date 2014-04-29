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
package tbd.mod

import akka.actor.ActorRef
import scala.collection.mutable.{ArrayBuffer, Buffer, Set}

import tbd.{Changeable, TBD}
import tbd.datastore.Datastore

class PartitionedDoubleModList[T](
    aPartitions: ArrayBuffer[DoubleModList[T]]) extends ModList[T] {
  val partitions = aPartitions

  def map[U](tbd: TBD, f: T => U): PartitionedDoubleModList[U] = {
    new PartitionedDoubleModList(
      partitions.map((partition: DoubleModList[T]) => {
        partition.map(tbd, f)
      })
    )
  }

  def memoMap[U](tbd: TBD, f: T => U): ModList[U] = {
    new PartitionedDoubleModList(
      partitions.map((partition: DoubleModList[T]) => {
        partition.memoMap(tbd, f)
      })
    )
  }

  def parMap[U](tbd: TBD, f: (TBD, T) => U): ModList[U] = {
    def innerParMap(tbd: TBD, i: Int): ArrayBuffer[DoubleModList[U]] = {
      if (i < partitions.size) {
        val parTup = tbd.par((tbd: TBD) => {
          partitions(i).map(tbd, (value: T) => f(tbd, value))
        }, (tbd: TBD) => {
          innerParMap(tbd, i + 1)
        })

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[DoubleModList[U]]()
      }
    }
      
    new PartitionedDoubleModList(innerParMap(tbd, 0))
  }

  def memoParMap[U](tbd: TBD, f: (TBD, T) => U): ModList[U] = {
    def innerMemoParMap(tbd: TBD, i: Int): ArrayBuffer[DoubleModList[U]] = {
      if (i < partitions.size) {
        val parTup = tbd.par((tbd: TBD) => {
          partitions(i).memoMap(tbd, (value: T) => f(tbd, value))
        }, (tbd: TBD) => {
          innerMemoParMap(tbd, i + 1)
        })

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[DoubleModList[U]]()
      }
    }

    new PartitionedDoubleModList(innerMemoParMap(tbd, 0))
  }

  /* Meta Operations */
  def toBuffer(): Buffer[T] = {
    val buf = ArrayBuffer[T]()

    for (partition <- partitions) {
      var innerNode = partition.head.read()
      while (innerNode != null) {
        buf += innerNode.value.read()
        innerNode = innerNode.next.read()
      }
    }

    buf
  }
}
