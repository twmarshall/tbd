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

class PartitionedModList[T, V](
    val partitions: ArrayBuffer[ModList[T, V]]
  ) extends AdjustableList[T, V] {

  def map[U, Q](
      f: (TBD, (T, V)) => (U, Q),
      parallel: Boolean = true)
     (implicit tbd: TBD): PartitionedModList[U, Q] = {
    if (parallel) {
      def innerMap(i: Int)(implicit tbd: TBD): ArrayBuffer[ModList[U, Q]] = {
        if (i < partitions.size) {
          val parTup = par((tbd: TBD) => {
            partitions(i).map(f)(tbd)
          }, (tbd: TBD) => {
            innerMap(i + 1)(tbd)
          })

          parTup._2 += parTup._1
        } else {
          ArrayBuffer[ModList[U, Q]]()
        }
      }

      new PartitionedModList(innerMap(0))
    } else {
      new PartitionedModList(
        partitions.map((partition: ModList[T, V]) => {
          partition.map(f)
        })
      )
    }
  }

  def reduce(
      initialValueMod: Mod[(T, V)],
      f: (TBD, (T, V), (T, V)) => (T, V),
      parallel: Boolean = true)
     (implicit tbd: TBD): Mod[(T, V)] = {

    def parReduce(i: Int)(implicit tbd: TBD): Mod[(T, V)] = {
      if (i < partitions.size) {
        val parTup = par((tbd: TBD) => {
          partitions(i).reduce(initialValueMod, f, parallel)(tbd)
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
      partitions.map((partition: ModList[T, V]) => {
        partition.reduce(initialValueMod, f, parallel)
      }).reduce((a, b) => {
        mod {
          read2(a, b) {
	    case (a, b) => write(f(tbd, a, b))
          }
        }
      })
    }
  }

  def filter(
      pred: ((T, V)) => Boolean,
      parallel: Boolean = true)
     (implicit tbd: TBD): PartitionedModList[T, V] = {
    def parFilter(i: Int)(implicit tbd: TBD): ArrayBuffer[ModList[T, V]] = {
      if (i < partitions.size) {
        val parTup = par((tbd: TBD) => {
          partitions(i).filter(pred, parallel)(tbd)
        }, (tbd: TBD) => {
          parFilter(i + 1)(tbd)
        })

        parTup._2 += parTup._1
      } else {
        ArrayBuffer[ModList[T, V]]()
      }
    }

    if (parallel) {
      new PartitionedModList(parFilter(0))
    } else {
      new PartitionedModList(
        partitions.map((partition: ModList[T, V]) => {
          partition.filter(pred, parallel)
        })
      )
    }
  }

  def split(
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false)
     (implicit tbd: TBD): (AdjustableList[T, V], AdjustableList[T, V]) = ???

  def sort(
      comperator: (TBD, (T, V), (T, V)) => Boolean,
      parallel: Boolean = false)
     (implicit tbd: TBD): AdjustableList[T, V] = ???


  /* Meta Operations */
  def toBuffer(): Buffer[V] = {
    val buf = ArrayBuffer[V]()

    for (partition <- partitions) {
      var innerNode = partition.head.read()
      while (innerNode != null) {
        buf += innerNode.value._2
        innerNode = innerNode.next.read()
      }
    }

    buf
  }
}
