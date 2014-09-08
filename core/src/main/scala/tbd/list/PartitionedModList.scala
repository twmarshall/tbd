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

import akka.actor.ActorRef
import scala.collection.mutable.{Buffer, Set}

import tbd._
import tbd.datastore.Datastore
import tbd.TBD._

class PartitionedModList[T, U](
    val partitions: Buffer[ModList[T, U]]
  ) extends AdjustableList[T, U] {

  def filter(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): PartitionedModList[T, U] = {
    def parFilter(i: Int)(implicit c: Context): Buffer[ModList[T, U]] = {
      if (i < partitions.size) {
        val (filteredPartition, filteredRest) = par {
          c => partitions(i).filter(pred)(c)
        } and {
          c => parFilter(i + 1)(c)
        }

	filteredRest += filteredPartition
      } else {
        Buffer[ModList[T, U]]()
      }
    }

    new PartitionedModList(parFilter(0))
  }

  def flatMap[V, W](
      f: ((T, U)) => Iterable[(V, W)])
     (implicit c: Context): PartitionedModList[V, W] = {
    def innerFlatMap(i: Int)(implicit c: Context): Buffer[ModList[V, W]] = {
      if (i < partitions.size) {
        val (mappedPartition, mappedRest) = par {
          c => partitions(i).flatMap(f)(c)
        } and {
          c => innerFlatMap(i + 1)(c)
        }

	mappedRest += mappedPartition
      } else {
        Buffer[ModList[V, W]]()
      }
    }

    new PartitionedModList(innerFlatMap(0))
  }

  def join[V](
      that: AdjustableList[T, V])
     (implicit c: Context): PartitionedModList[T, (U, V)] = ???

  def map[V, W](
      f: ((T, U)) => (V, W))
     (implicit c: Context): PartitionedModList[V, W] = {
    def innerMap(i: Int)(implicit c: Context): Buffer[ModList[V, W]] = {
      if (i < partitions.size) {
        val (mappedPartition, mappedRest) = par {
          c => partitions(i).map(f)(c)
        } and {
          c => innerMap(i + 1)(c)
        }

	mappedRest += mappedPartition
      } else {
        Buffer[ModList[V, W]]()
      }
    }

    new PartitionedModList(innerMap(0))
  }

  override def mergesort()
      (implicit c: Context,
       ordering: Ordering[T]): PartitionedModList[T, U] = {
    def innerSort(i: Int)(implicit c: Context): ModList[T, U] = {
      if (i < partitions.size) {
        val (sortedPartition, sortedRest) = par {
          c => partitions(i).mergesort()(c, ordering)
        } and {
          c => innerSort(i + 1)(c)
        }

	sortedPartition.merge(sortedRest, (pair1, pair2) => ordering.lt(pair1._1, pair2._1))
      } else {
        new ModList[T, U](mod { write[ModListNode[T, U]](null) })
      }
    }

    // TODO: make the output have as many partitions as this list.
    new PartitionedModList(Buffer(innerSort(0)))
  }

  override def quicksort()
      (implicit c: Context,
       ordering: Ordering[T]): PartitionedModList[T, U] = {
    def innerSort(i: Int)(implicit c: Context): ModList[T, U] = {
      if (i < partitions.size) {
        val (sortedPartition, sortedRest) = par {
          c => partitions(i).quicksort()(c, ordering)
        } and {
          c => innerSort(i + 1)(c)
        }

	sortedPartition.merge(sortedRest, (pair1, pair2) => ordering.lt(pair1._1, pair2._1))
      } else {
        new ModList[T, U](mod { write[ModListNode[T, U]](null) })
      }
    }

    // TODO: make the output have as many partitions as this list.
    new PartitionedModList(Buffer(innerSort(0)))
  }

  def reduce(
      f: ((T, U), (T, U)) => (T, U))
     (implicit c: Context): Mod[(T, U)] = {
    def innerReduce(i: Int)(implicit c: Context): Mod[(T, U)] = {
      if (i < partitions.size) {
        val (reducedPartition, reducedRest) = par {
          c => partitions(i).reduce(f)(c)
        } and {
          c => innerReduce(i + 1)(c)
        }

        mod {
          read(reducedPartition) {
            case null =>
              read(reducedRest) {
                case null => write(null)
                case rest => write(rest)
              }
            case partition =>
              read(reducedRest) {
                case null => write(partition)
                case rest => write(f(partition, rest))
              }
          }
        }
      } else {
        mod { write[(T, U)](null) }
      }
    }

    innerReduce(0)
  }

  def sortJoin[V]
      (that: AdjustableList[T, V])
      (implicit c: Context, ordering: Ordering[T]): AdjustableList[T, (U, V)] = ???

  def split(
      pred: ((T, U)) => Boolean)
     (implicit c: Context
    ): (PartitionedModList[T, U], PartitionedModList[T, U]) = ???

  /* Meta Operations */
  def toBuffer(): Buffer[(T, U)] = {
    val buf = Buffer[(T, U)]()

    for (partition <- partitions) {
      buf ++= partition.toBuffer()
    }

    buf
  }
}
