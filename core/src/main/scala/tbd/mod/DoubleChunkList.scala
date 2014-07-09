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
import tbd.Constants._

class DoubleChunkList[T, U](
    _head: Mod[DoubleChunkListNode[T, U]]) extends AdjustableChunkList[T, U] {
  val head = _head

  def map[V, Q](
      tbd: TBD,
      f: (TBD, (T, U)) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleChunkList[V, Q] = {
    if (parallel) {
      new DoubleChunkList(
        tbd.mod((dest: Dest[DoubleChunkListNode[V, Q]]) => {
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
      val memo = tbd.makeMemoizer[Mod[DoubleChunkListNode[V, Q]]](!memoized)
      new DoubleChunkList(
        tbd.mod((dest: Dest[DoubleChunkListNode[V, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.map(tbd, dest, f, memo)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }

  def chunkMap[V, Q](
      tbd: TBD,
      f: (TBD, Vector[(T, U)]) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[V, Q] = {
    if (parallel || !memoized) {
      tbd.log.warning("DoubleChunkList.chunkMap ignores the 'parallel' and " +
		      "'memoized' parameters.")
    }

    val memo = tbd.makeMemoizer[Mod[DoubleModListNode[V, Q]]]()
    new DoubleModList(
      tbd.mod((dest: Dest[DoubleModListNode[V, Q]]) => {
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
      memoized: Boolean = true): DoubleChunkList[T, U] = ???

  def reduce(
      tbd: TBD,
      _identityMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean = false,
      memoized: Boolean = true): Mod[(T, U)] = {

    val identityMod = tbd.mod((dest: Dest[Vector[(T, U)]]) => {
      tbd.read(_identityMod)(_identityValue => {
	tbd.write(dest, Vector(_identityValue))
      })
    })

    // Each round we need a hasher and a memo, and we need to guarantee that the
    // same hasher and memo are used for a given round during change propagation,
    // even if the first mod of the list is deleted.
    class RoundMemoizer {
      val memo = tbd.makeMemoizer[(Hasher,
                               Memoizer[Mod[DoubleChunkListNode[T, U]]],
                               RoundMemoizer)](!memoized)

      def getTuple() =
        memo() {
          (new Hasher(2, 4),
           tbd.makeMemoizer[Mod[DoubleChunkListNode[T, U]]](!memoized),
           new RoundMemoizer())
	}
    }

    def randomReduceList(
        head: DoubleChunkListNode[T, U],
        round: Int,
        dest: Dest[(T, U)],
        roundMemoizer: RoundMemoizer): Changeable[(T, U)] = {
      val tuple = roundMemoizer.getTuple()

      val halfListMod =
        tbd.mod((dest: Dest[DoubleChunkListNode[T, U]]) =>
          halfList(identityMod, head, round, tuple._1, tuple._2, dest))

      tbd.read(halfListMod)(halfList =>
        tbd.read(halfList.nextMod)(next =>
          if(next == null)
            tbd.read(halfList.chunkMod)(chunk =>
              tbd.write(dest, chunk.reduce(f(tbd, _, _))))
          else
            randomReduceList(halfList, round + 1, dest, tuple._3)))
    }

    def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
      hasher.hash(id.hashCode() ^ round) == 0
    }

    def halfList(
        acc: Mod[Vector[(T, U)]],
        head: DoubleChunkListNode[T, U],
        round: Int,
        hasher: Hasher,
        memo: Memoizer[Mod[DoubleChunkListNode[T, U]]],
        dest: Dest[DoubleChunkListNode[T, U]])
          : Changeable[DoubleChunkListNode[T, U]] = {
      val newAcc = tbd.mod((dest: Dest[Vector[(T, U)]]) =>
        tbd.read(acc)((acc) =>
	  tbd.read(head.chunkMod)(chunk =>
	    if (chunk.size > 0)
              tbd.write(dest, Vector(f(tbd, acc(0), chunk.reduce(f(tbd, _, _)))))
	    else
	      tbd.write(dest, Vector(acc(0))))))

      if(binaryHash(head.chunkMod.id, round, hasher)) {
        val newNext = memo(head.nextMod, identityMod) {
	  tbd.mod((dest: Dest[DoubleChunkListNode[T, U]]) =>
	    tbd.read(head.nextMod)(next =>
	      if (next == null)
	        tbd.write(dest, null)
	      else
	        halfList(identityMod, next, round, hasher, memo, dest)))
	}
        tbd.write(dest, new DoubleChunkListNode(newAcc, newNext))
      } else {
        tbd.read(head.nextMod)(next =>
	  if (next == null) {
	    val newNext = tbd.createMod[DoubleChunkListNode[T, U]](null)
            tbd.write(dest, new DoubleChunkListNode(newAcc, newNext))
	  } else {
	    halfList(newAcc, next, round, hasher, memo, dest)
	  }
        )
      }
    }

    val roundMemoizer = new RoundMemoizer()
    tbd.mod((dest: Dest[(T, U)]) =>
      tbd.read(head)(head =>
        if(head == null)
          tbd.read(identityMod)(identity =>
            tbd.write(dest, identity(0)))
        else
          randomReduceList(head, 0, dest, roundMemoizer)))
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

  /* Meta functions */
  def toBuffer(): Buffer[U] = {
    val buf = Buffer[U]()
    var node = head.read()
    while (node != null) {
      buf ++= node.chunkMod.read().map(value => value._2)
      node = node.nextMod.read()
    }

    buf
  }
}
