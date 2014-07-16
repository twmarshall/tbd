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

import scala.collection.mutable.{ArrayBuffer, Buffer}

import tbd.{Changeable, Changeable2, Memoizer, TBD}
import tbd.Constants.ModId

class DoubleModList[T, V](
    val head: Mod[DoubleModListNode[T, V]]
  ) extends AdjustableList[T, V]
    with Iterable[T, V, DoubleModListNode[T, V]] {

  def iterators(tbd: TBD) = {
    List(head)
  }

  def map[U, Q](
      tbd: TBD,
      f: (TBD, (T, V)) => (U, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[U, Q] = {

    if (parallel) {
      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) => {
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
      val memo = tbd.makeMemoizer[(Mod[(U, Q)], Mod[DoubleModListNode[U, Q]])](!memoized)

      new DoubleModList(
        tbd.modNoDest(() => {
          tbd.read(head)(node => {
            if (node != null) {
              node.map(tbd, f, memo)
            } else {
              tbd.writeNoDest(null)
            }
          })
        })
      )
    }
  }

  def split(
      tbd: TBD,
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
       (AdjustableList[T, V], AdjustableList[T, V]) = {

    //val memo = tbd.makeMemoizer[(Mod[DoubleModListNode[T, V]],
    //                         Mod[DoubleModListNode[T, V]])](!memoized)

    /*val result = tbd.mod2((matches: Dest[DoubleModListNode[T, V]],
                             diffs: Dest[DoubleModListNode[T, V]]) => {

        tbd.read(head)(head => {
          if(head == null) {
            tbd.write(matches, null)
            tbd.write(diffs, null)
          } else {
            head.split(tbd, matches, diffs, memo, pred)
          }
        })
      })*/

    val memo = tbd.makeMemoizer[Changeable2[DoubleModListNode[T, V], DoubleModListNode[T, V]]](!memoized)

    val result = tbd.modNoDest2[DoubleModListNode[T, V], DoubleModListNode[T, V]](() => {
      tbd.read(head)(head => {
	if (head == null) {
	  tbd.writeNoDest2(null.asInstanceOf[DoubleModListNode[T, V]],
			   null.asInstanceOf[DoubleModListNode[T, V]])
	} else {
	  head.splitNoDest(tbd, memo, pred)
	}
      })
    })

    (new DoubleModList(result._1), new DoubleModList(result._2))
  }

  def sort(
      tbd: TBD,
      comperator: (TBD, (T, V), (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): AdjustableList[T, V] = {

    val memo = tbd.makeMemoizer[Mod[DoubleModListNode[T, V]]](!memoized)

    val sorted = tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
      tbd.read(head)(head => {
        if(head == null) {
          tbd.write(dest, null)
        } else {
          head.quicksort(tbd, dest, tbd.createMod(null),
                         comperator, memo, parallel, memoized)
        }
      })
    })

    new DoubleModList(sorted)
  }

  def reduce(
      tbd: TBD,
      identityMod: Mod[(T, V)],
      f: (TBD, (T, V), (T, V)) => (T, V),
      parallel: Boolean = false,
      memoized: Boolean = true): Mod[(T, V)] = {
    // Each round we need a hasher and a memo, and we need to guarantee that the
    // same hasher and memo are used for a given round during change propagation,
    // even if the first mod of the list is deleted.
    class RoundMemo {
      val memo = tbd.makeMemoizer[(Hasher,
                               Memoizer[Mod[DoubleModListNode[T, V]]],
                               RoundMemo)](!memoized)

      def getTuple() =
        memo() {
          (new Hasher(2, 4),
           tbd.makeMemoizer[Mod[DoubleModListNode[T, V]]](!memoized),
           new RoundMemo())
	}
    }

    def randomReduceList(
        head: DoubleModListNode[T, V],
        round: Int,
        roundMemo: RoundMemo): Changeable[(T, V)] = {
      val tuple = roundMemo.getTuple()

      val halfListMod =
        tbd.modNoDest(() =>
          halfList(identityMod, head, round, tuple._1, tuple._2))

      tbd.read(halfListMod)(halfList =>
        tbd.read(halfList.next)(next =>
          if(next == null)
            tbd.read(halfList.value)(value =>
              tbd.writeNoDest(value))
          else
            randomReduceList(halfList, round + 1, tuple._3)))
    }

    def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
      hasher.hash(id.hashCode() ^ round) == 0
    }

    def halfList(
        acc: Mod[(T, V)],
        head: DoubleModListNode[T, V],
        round: Int,
        hasher: Hasher,
        memo: Memoizer[Mod[DoubleModListNode[T, V]]])
          : Changeable[DoubleModListNode[T, V]] = {
      val newAcc = tbd.modNoDest[(T, V)](() =>
        tbd.read(acc)((acc) =>
	  tbd.read(head.value)(value =>
            tbd.writeNoDest(f(tbd, acc, value)))))

      if(binaryHash(head.value.id, round, hasher)) {
        val newNext = memo(List(head.next, identityMod)) {
	  tbd.modNoDest(() =>
	    tbd.read(head.next)(next =>
	      if (next == null)
	        tbd.writeNoDest(null)
	      else
	        halfList(identityMod, next, round, hasher, memo)))
	}
        tbd.writeNoDest(new DoubleModListNode(newAcc, newNext))
      } else {
        tbd.read(head.next)(next =>
	  if (next == null) {
	    val newNext = tbd.createMod[DoubleModListNode[T, V]](null)
            tbd.writeNoDest(new DoubleModListNode(newAcc, newNext))
	  } else {
	    halfList(newAcc, next, round, hasher, memo)
	  }
        )
      }
    }

    val roundMemo = new RoundMemo()
    tbd.modNoDest(() =>
      tbd.read(head)(head =>
        if(head == null)
          tbd.read(identityMod)(identity =>
            tbd.writeNoDest(identity))
        else
          randomReduceList(head, 0, roundMemo)))
  }

  def filter(
      tbd: TBD,
      pred: ((T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): DoubleModList[T, V] = {
    val memo = tbd.makeMemoizer[Mod[DoubleModListNode[T, V]]](!memoized)

    new DoubleModList(
      tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
        tbd.read(head)(node => {
          if (node != null) {
            node.filter(tbd, dest, pred, memo)
          } else {
            tbd.write(dest, null)
          }
        })
      })
    )
  }

  def toBuffer(): Buffer[V] = {
    val buf = ArrayBuffer[V]()
    var node = head.read()
    while (node != null) {
      buf += node.value.read()._2
      node = node.next.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
