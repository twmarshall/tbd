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

import scala.collection.mutable.Buffer

import tbd.{Changeable, Memoizer, TBD}
import tbd.Constants.ModId

class ChunkList[T, U](
    val head: Mod[ChunkListNode[T, U]]) extends AdjustableChunkList[T, U] {

  def map[V, W](
      tbd: TBD,
      f: (TBD, (T, U)) => (V, W),
      parallel: Boolean = false,
      memoized: Boolean = true): ChunkList[V, W] = {
    if (parallel || !memoized) {
      tbd.log.warning("ChunkList.map ignores the 'parallel' and " +
		      "'memoized' parameters.")
    }

    val memo = tbd.makeMemoizer[Changeable[ChunkListNode[V, W]]]()
    new ChunkList(
      tbd.mod {
        tbd.read(head)(node => {
          if (node != null) {
            node.map(tbd, f, memo)
          } else {
            tbd.write[ChunkListNode[V, W]](null)
          }
        })
      }
    )
  }

  def chunkMap[V, Q](
      tbd: TBD,
      f: (TBD, Vector[(T, U)]) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): ModList[V, Q] = {
    if (parallel || !memoized) {
      tbd.log.warning("ChunkList.chunkMap ignores the 'parallel' and " +
		      "'memoized' parameters.")
    }

    val memo = tbd.makeMemoizer[Mod[ModListNode[V, Q]]]()
    new ModList(
      tbd.mod((dest: Dest[ModListNode[V, Q]]) => {
        tbd.read(head)(node => {
          if (node != null) {
            node.chunkMap(tbd, dest, f, memo)
          } else {
            tbd.write(dest, null)
          }
        })
      })
    )
  }

  def filter(
      tbd: TBD,
      pred: ((T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): ChunkList[T, U] = ???

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean = false,
      memoized: Boolean = true): Mod[(T, U)] = ???

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
      parallel: Boolean = false): Mod[(Int, Vector[(T, U)])] = {
    println("chunkSort")
    def mapper(tbd: TBD, chunk: Vector[(T, U)]): (Int, Vector[(T, U)]) = {
      (0, chunk.sortWith((pair1: (T, U), pair2: (T, U)) => {
	comparator(tbd, pair1, pair2)
      }))
    }
    val sortedChunks = chunkMap(tbd, mapper)

    def reducer(tbd: TBD, pair1: (Int, Vector[(T, U)]), pair2: (Int, Vector[(T, U)])) = {
      def innerReducer(v1: Vector[(T, U)], v2: Vector[(T, U)]): Vector[(T, U)] = {
	if (v1.size == 0) {
	  v2
	} else if (v2.size == 0) {
	  v1
	} else {
	  if (comparator(tbd, v1.head, v2.head)) {
	    v1.head +: innerReducer(v1.tail, v2)
	  } else {
	    v2.head +: innerReducer(v1, v2.tail)
	  }
	}
      }
      (pair1._1, innerReducer(pair1._2, pair2._2))
    }

    val initialValue = tbd.createMod((0, Vector[(T, U)]()))
    sortedChunks.reduce(tbd, initialValue, reducer)
  }

  /* Meta functions */
  def toBuffer(): Buffer[U] = {
    val buf = Buffer[U]()
    var node = head.read()
    while (node != null) {
      buf ++= node.chunk.map(value => value._2)
      node = node.nextMod.read()
    }

    buf
  }
}
