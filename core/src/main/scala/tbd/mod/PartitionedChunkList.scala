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

  def chunkMap[V, W](
      f: (TBD, Vector[(T, U)]) => (V, W))
     (implicit tbd: TBD): PartitionedModList[V, W] = {
    def innerChunkMap(i: Int)(implicit tbd: TBD): ArrayBuffer[ModList[V, W]] = {
      if (i < partitions.size) {
        val parTup = par {
          tbd => partitions(i).chunkMap(f)(tbd)
        } and {
          tbd => innerChunkMap(i + 1)(tbd)
        }

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ModList[V, W]]()
      }
    }

    new PartitionedModList(innerChunkMap(0))
  }

  def chunkSort(
      comparator: (TBD, (T, U), (T, U)) => Boolean)
     (implicit tbd: TBD): Mod[(Int, Vector[(T, U)])] = ???

  def filter(
      pred: ((T, U)) => Boolean)
     (implicit tbd: TBD): PartitionedChunkList[T, U] = {
    def parFilter(i: Int)(implicit tbd: TBD): ArrayBuffer[ChunkList[T, U]] = {
      if (i < partitions.size) {
        val parTup = par {
          tbd => partitions(i).filter(pred)(tbd)
	} and {
          tbd => parFilter(i + 1)(tbd)
        }

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ChunkList[T, U]]()
      }
    }

    new PartitionedChunkList(parFilter(0))
  }

  def map[V, W](
      f: (TBD, (T, U)) => (V, W))
     (implicit tbd: TBD): PartitionedChunkList[V, W] = {
    def innerMap(i: Int)(implicit tbd: TBD): ArrayBuffer[ChunkList[V, W]] = {
      if (i < partitions.size) {
        val parTup = par {
          tbd => partitions(i).map(f)(tbd)
        } and {
          tbd => innerMap(i + 1)(tbd)
        }

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ChunkList[V, W]]()
      }
    }

    new PartitionedChunkList(innerMap(0))
  }

  def reduce(
      initialValueMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U))
     (implicit tbd: TBD): Mod[(T, U)] = {

    def parReduce(i: Int)(implicit tbd: TBD): Mod[(T, U)] = {
      if (i < partitions.size) {
        val parTup = par {
          tbd => partitions(i).reduce(initialValueMod, f)(tbd)
        } and {
          tbd => parReduce(i + 1)(tbd)
        }

        mod {
          read2(parTup._1, parTup._2) {
	    case (a, b) => write(f(tbd, a, b))
          }
        }
      } else {
        initialValueMod
      }
    }

    parReduce(0)
  }

  def sort(
      comperator: (TBD, (T, U), (T, U)) => Boolean)
     (implicit tbd: TBD): AdjustableList[T, U] = ???

  def split(
      pred: (TBD, (T, U)) => Boolean)
     (implicit tbd: TBD): (AdjustableList[T, U], AdjustableList[T, U]) = ???

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
