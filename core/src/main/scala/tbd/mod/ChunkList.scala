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

import tbd.{Changeable, TBD}
import tbd.Constants._
import tbd.memo.Lift

class ChunkList[T, U](
    _head: Mod[ChunkListNode[T, U]]) extends AdjustableChunkList[T, U] {
  val head = _head

  def map[V, Q](
      tbd: TBD,
      f: (TBD, (T, U)) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): ChunkList[V, Q] = ??? /*{
    if (parallel) {
      new ChunkList(
        tbd.mod((dest: Dest[ChunkListNode[V, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.parMap(tbd, dest, f)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    } else {
      val lift = tbd.makeLift[Mod[ChunkListNode[V, Q]]](!memoized)
      new ChunkList(
        tbd.mod((dest: Dest[ChunkListNode[V, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.map(tbd, dest, f, lift)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }*/

  def chunkMap[V, Q](
      tbd: TBD,
      f: (TBD, Vector[(T, U)]) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[V, Q] = ???

  def filter(
      tbd: TBD,
      pred: ((T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): ChunkList[T, U] = ???

  def randomReduce(
      tbd: TBD,
      initialValueMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean,
      memoized: Boolean): Mod[(T, U)] = ??? /*{
    val zero = 0
    val halfLift = tbd.makeLift[Mod[ChunkListNode[T, U]]](!memoized)

    val identityMod = tbd.mod((dest: Dest[Vector[(T, U)]]) => {
      tbd.read(initialValueMod)(initialValue => {
	tbd.write(dest, Vector(initialValue))
      })
    })

    tbd.mod((dest: Dest[(T, U)]) => {
      tbd.read(head)(h => {
        if(h == null) {
          tbd.read(initialValueMod)(initialValue =>
            tbd.write(dest, initialValue))
        } else {
          randomReduceList(tbd, identityMod,
                           h, head, zero, dest,
                           halfLift, f)
        }
      })
    })
  }

  val hasher = new Hasher(2, 8)
  def randomReduceList(
      tbd: TBD,
      identityMod: Mod[Vector[(T, U)]],
      head: ChunkListNode[T, U],
      headMod: Mod[ChunkListNode[T, U]],
      round: Int,
      dest: Dest[(T, U)],
      lift: Lift[Mod[ChunkListNode[T, U]]],
      f: (TBD, (T, U), (T, U)) => (T, U)): Changeable[(T, U)] = {
    val halfListMod =
        tbd.mod((dest: Dest[ChunkListNode[T, U]]) => {
          halfList(tbd, identityMod, identityMod,
                   head, round, new Hasher(2, 8),
                   lift, dest, f)
      })

    tbd.read(halfListMod)(halfList => {
      tbd.read(halfList.nextMod)(next => {
        if(next == null) {
          tbd.read(halfList.chunkMod)(chunk =>
            tbd.write(dest, chunk.reduce(f(tbd, _, _))))
        } else {
            randomReduceList(tbd, identityMod, halfList, halfListMod,
                             round + 1, dest,
                             lift, f)
        }
      })
    })
  }

  def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
    hasher.hash(id.hashCode() ^ round) == 0
  }

  def halfList(
      tbd: TBD,
      identityMod: Mod[Vector[(T, U)]],
      acc: Mod[Vector[(T, U)]],
      head: ChunkListNode[T, U],
      round: Int,
      hasher: Hasher,
      lift: Lift[Mod[ChunkListNode[T, U]]],
      dest: Dest[ChunkListNode[T, U]],
      f: (TBD, (T, U), (T, U)) => (T, U)): Changeable[ChunkListNode[T, U]] = {
    val newAcc = tbd.mod((dest: Dest[Vector[(T, U)]]) =>
      tbd.read(acc)((acc) =>
	tbd.read(head.chunkMod)(chunk =>
	  if (chunk.size > 0)
            tbd.write(dest, Vector(f(tbd, acc(0), chunk.reduce(f(tbd, _, _)))))
	  else
	    tbd.write(dest, Vector(acc(0)))
	)))

    if(binaryHash(head.chunkMod.id, round, hasher)) {
      val newNext =
	tbd.mod((dest: Dest[ChunkListNode[T, U]]) =>
	  tbd.read(head.nextMod)(next =>
	    if (next == null)
	      tbd.write(dest, null)
	    else
	      halfList(tbd, identityMod, identityMod,
		       next, round, hasher, lift, dest, f)))
      tbd.write(dest, new ChunkListNode(newAcc, newNext))
    } else {
      tbd.read(head.nextMod)(next => {
	if (next == null) {
	  val newNext = tbd.createMod[ChunkListNode[T, U]](null)
          tbd.write(dest, new ChunkListNode(newAcc, newNext))
	} else {
	  halfList(tbd, identityMod, newAcc,
		   next, round, hasher, lift, dest, f)
	}
      })
    }
  }*/

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean = false,
      memoized: Boolean = true): Mod[(T, U)] = ??? /* {
    randomReduce(tbd, initialValueMod, f, parallel, memoized)
  }*/

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
