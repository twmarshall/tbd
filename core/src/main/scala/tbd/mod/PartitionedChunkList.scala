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
import tbd.TBD._

class PartitionedChunkList[T, U](
    val partitions: ArrayBuffer[ChunkList[T, U]]
  ) extends AdjustableChunkList[T, U] {

  def map[V, Q](
      f: (TBD, (T, U)) => (V, Q),
      parallel: Boolean = true,
      memoized: Boolean = false)(implicit tbd: TBD): PartitionedChunkList[V, Q] = {
    if (parallel) {
      def innerMap(i: Int)(implicit tbd: TBD): ArrayBuffer[ChunkList[V, Q]] = {
        if (i < partitions.size) {
          val parTup = par((tbd: TBD) => {
            partitions(i).map(f, memoized = memoized)(tbd)
          }, (tbd: TBD) => {
            innerMap(i + 1)(tbd)
          })

          parTup._2 += parTup._1
        } else {
          ArrayBuffer[ChunkList[V, Q]]()
        }
      }

      new PartitionedChunkList(innerMap(0))
    } else {
      new PartitionedChunkList(
        partitions.map((partition: ChunkList[T, U]) => {
          partition.map(f, memoized = memoized)
        })
      )
    }
  }

  def chunkMap[V, Q](
      tbd: TBD,
      f: (TBD, Vector[(T, U)]) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): PartitionedModList[V, Q] = {
    if (parallel) {
      def innerChunkMap(tbd: TBD, i: Int): ArrayBuffer[ModList[V, Q]] = {
        if (i < partitions.size) {
          val parTup = tbd.par((tbd: TBD) => {
            partitions(i).chunkMap(tbd, f, memoized = memoized)
          }, (tbd: TBD) => {
            innerChunkMap(tbd, i + 1)
          })

          parTup._2 += parTup._1
        } else {
          ArrayBuffer[ModList[V, Q]]()
        }
      }

      new PartitionedModList(innerChunkMap(tbd, 0))
    } else {
      new PartitionedModList(
        partitions.map((partition: ChunkList[T, U]) => {
          partition.chunkMap(tbd, f, memoized = memoized)
        })
      )
    }
  }

  def reduce(
      initialValueMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean = true,
      memoized: Boolean = true)
     (implicit tbd: TBD): Mod[(T, U)] = {

    def parReduce(i: Int)(implicit tbd: TBD): Mod[(T, U)] = {
      if (i < partitions.size) {
        val parTup = par((tbd: TBD) => {
          partitions(i).reduce(initialValueMod, f, parallel, memoized)(tbd)
        }, (tbd: TBD) => {
          parReduce(i + 1)(tbd)
        })

        mod {
          read2(parTup._1, parTup._2) {
	    case (a, b) => write(f(tbd, a, b))
          }
        }
      } else {
        initialValueMod
      }
    }

    if(parallel) {
      parReduce(0)
    } else {
      partitions.map((partition: ChunkList[T, U]) => {
        partition.reduce(initialValueMod, f, parallel, memoized)
      }).reduce((a, b) => {
        mod {
          read2(a, b) {
	    case (a, b) => tbd.write(f(tbd, a, b))
          }
        }
      })
    }
  }

  def filter(
      pred: ((T, U)) => Boolean,
      parallel: Boolean = true,
      memoized: Boolean = true)
     (implicit tbd: TBD): PartitionedChunkList[T, U] = {
    def parFilter(i: Int)(implicit tbd: TBD): ArrayBuffer[ChunkList[T, U]] = {
      if (i < partitions.size) {
        val parTup = par((tbd: TBD) => {
          partitions(i).filter(pred, parallel, memoized)(tbd)
        }, (tbd: TBD) => {
          parFilter(i + 1)(tbd)
        })

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ChunkList[T, U]]()
      }
    }

    if (parallel) {
      new PartitionedChunkList(parFilter(0))
    } else {
      new PartitionedChunkList(
        partitions.map((partition: ChunkList[T, U]) => {
          partition.filter(pred, parallel, memoized)
        })
      )
    }
  }

  def split(
      tbd: TBD,
      pred: (TBD, (T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
       (AdjustableList[T, U], AdjustableList[T, U]) = ???

  def sort(
      tbd: TBD,
      comperator: (TBD, (T, U), (T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): AdjustableList[T, U] = ???

  def chunkSort(
      tbd: TBD,
      comparator: (TBD, (T, U), (T, U)) => Boolean,
      parallel: Boolean = false): Mod[(Int, Vector[(T, U)])] = ???

  /* Meta Operations */
  def toBuffer(): Buffer[U] = {
    val buf = ArrayBuffer[U]()

    for (partition <- partitions) {
      var innerNode = partition.head.read()
      while (innerNode != null) {
        buf ++= innerNode.chunk.map(pair => pair._2)
        innerNode = innerNode.nextMod.read()
      }
    }

    buf
  }
}
