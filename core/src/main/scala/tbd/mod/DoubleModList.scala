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

import tbd.{Changeable, TBD}
import tbd.memo.Lift

class DoubleModList[T, V](
    aHead: Mod[DoubleModListNode[T, V]])
    extends AdjustableList[T, V]
    with Iterable[T, V, DoubleModListNode[T, V]] {
  val head = aHead

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
      val lift = tbd.makeLift[Mod[DoubleModListNode[U, Q]]](!memoized)

      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) => {
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
  }

  def randomReduce(
      tbd: TBD,
      initialValueMod: Mod[(T, V)],
      f: (TBD, (T, V), (T, V)) => (T, V),
      parallel: Boolean,
      memoized: Boolean): Mod[(T, V)] = {
    val zero = 0
    val halfLift = tbd.makeLift[Mod[DoubleModListNode[T, V]]](!memoized)

    tbd.mod((dest: Dest[(T, V)]) => {
      tbd.read(head)(h => {
        if(h == null) {
          tbd.read(initialValueMod)(initialValue =>
            tbd.write(dest, initialValue))
        } else {
          randomReduceList(tbd, initialValueMod,
                           h, head, zero, dest,
                           halfLift, f)
        }
      })
    })
  }

  val hasher = new Hasher(2, 8)
  def randomReduceList(
      tbd: TBD,
      identityMod: Mod[(T, V)],
      head: DoubleModListNode[T, V],
      headMod: Mod[DoubleModListNode[T, V]],
      round: Int,
      dest: Dest[(T, V)],
      lift: Lift[Mod[DoubleModListNode[T, V]]],
      f: (TBD, (T, V), (T, V)) => (T, V)): Changeable[(T, V)] = {
    //println("random reduce list")
    val halfListMod = //lift.memo(List(headMod, identityMod), () => {
        tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
          halfList(tbd, identityMod, identityMod,
                   head, round, new Hasher(2, 8),
                   lift, dest, f)
      })
    //})

    tbd.read(halfListMod)(halfList => {
      tbd.read(halfList.next)(next => {
        if(next == null) {
          tbd.read(halfList.value)(value =>
            tbd.write(dest, value))
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
      identityMod: Mod[(T, V)],
      acc: Mod[(T, V)],
      head: DoubleModListNode[T, V],
      round: Int,
      hasher: Hasher,
      lift: Lift[Mod[DoubleModListNode[T, V]]],
      dest: Dest[DoubleModListNode[T, V]],
      f: (TBD, (T, V), (T, V)) => (T, V)): Changeable[DoubleModListNode[T, V]] = {
    //println("halfList")
    val newAcc = tbd.mod((dest: Dest[(T, V)]) =>
      tbd.read(acc)((acc) =>
	tbd.read(head.value)(value =>
          tbd.write(dest, f(tbd, acc, value)))))

    if(binaryHash(head.value.id, round, hasher)) {
      //println("looking for a match for " + head.next.id + " " + identityMod.id)
      val newNext = //lift.memo(List(head.next, identityMod), () =>
	tbd.mod((dest: Dest[DoubleModListNode[T, V]]) =>
	  tbd.read(head.next)(next =>
	    if (next == null)
	      tbd.write(dest, null)
	    else
	      halfList(tbd, identityMod, identityMod,
		       next, round, hasher, lift, dest, f))) //)
      tbd.write(dest, new DoubleModListNode(newAcc, newNext))
    } else {
      tbd.read(head.next)(next => {
	if (next == null) {
	  val newNext = tbd.createMod[DoubleModListNode[T, V]](null)
          tbd.write(dest, new DoubleModListNode(newAcc, newNext))
	} else {
	  halfList(tbd, identityMod, newAcc,
		   next, round, hasher, lift, dest, f)
	}
      })
    }
  }

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[(T, V)],
      f: (TBD, (T, V), (T, V)) => (T, V),
      parallel: Boolean = false,
      memoized: Boolean = true): Mod[(T, V)] = {
    randomReduce(tbd, initialValueMod, f, parallel, memoized)
  }

  def filter(
      tbd: TBD,
      pred: ((T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[T, V] = {
    val lift = tbd.makeLift[Mod[DoubleModListNode[T, V]]](!memoized)

    new DoubleModList(
      tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
        tbd.read(head)(node => {
          if (node != null) {
            node.filter(tbd, dest, pred, lift)
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
