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
import tbd.TBD._

class ModList[T, V](
    val head: Mod[ModListNode[T, V]]
  ) extends AdjustableList[T, V] {

  def map[U, Q](
      f: (TBD, (T, V)) => (U, Q),
      parallel: Boolean = false,
      memoized: Boolean = true)
     (implicit tbd: TBD): ModList[U, Q] = {
    if (parallel) {
      new ModList(
        mod {
          read(head) {
	    case null => write[ModListNode[U, Q]](null)
	    case node => node.parMap(tbd, f)
          }
        }
      )
    } else {
      val memo = makeMemoizer[Changeable[ModListNode[U, Q]]](!memoized)

      new ModList(
        mod {
          read(head) {
            case null => write[ModListNode[U, Q]](null)
            case node => node.map(f, memo)
          }
        }
      )
    }
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
    class RoundMemoizer {
      val memo = tbd.makeMemoizer[(Hasher,
                               Memoizer[Mod[ModListNode[T, V]]],
                               RoundMemoizer)](!memoized)

      def getTuple() =
        memo() {
          (new Hasher(2, 4),
           tbd.makeMemoizer[Mod[ModListNode[T, V]]](!memoized),
           new RoundMemoizer())
	}
    }

    def randomReduceList(
        head: ModListNode[T, V],
        identity: (T, V),
        round: Int,
        roundMemoizer: RoundMemoizer): Changeable[(T, V)] = {
      val tuple = roundMemoizer.getTuple()

      val halfListMod =
        tbd.mod {
          tbd.read(identityMod)(identity =>
            halfList(identity, identity, head, round, tuple._1, tuple._2))
	}

      tbd.read(halfListMod)(halfList =>
        tbd.read(halfList.next)(next =>
          if(next == null)
            tbd.write(halfList.value)
          else
            randomReduceList(halfList, identity, round + 1, tuple._3)))
    }

    def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
      hasher.hash(id.hashCode() ^ round) == 0
    }

    def halfList(
        acc: (T, V),
        identity: (T, V),
        head: ModListNode[T, V],
        round: Int,
        hasher: Hasher,
        memo: Memoizer[Mod[ModListNode[T, V]]]
      ): Changeable[ModListNode[T, V]] = {
      val newAcc = f(tbd, acc, head.value)

      if(binaryHash(head.next.id, round, hasher)) {
        val newNext = memo(head.next, identityMod) {
	  tbd.mod {
	    tbd.read(head.next)(next =>
	      if (next == null)
	        tbd.write[ModListNode[T, V]](null)
	      else
	          halfList(identity, identity, next, round,
                           hasher, memo))
	  }
	}
        tbd.write(new ModListNode(newAcc, newNext))
      } else {
        tbd.read(head.next)(next =>
	  if (next == null) {
	    val newNext = tbd.createMod[ModListNode[T, V]](null)
            tbd.write(new ModListNode(newAcc, newNext))
	  } else {
	    halfList(newAcc, identity, next, round, hasher, memo)
	  }
        )
      }
    }

    val roundMemoizer = new RoundMemoizer()
    tbd.mod {
      tbd.read(identityMod)(identity =>
        tbd.read(head)(head =>
          if(head == null)
            tbd.read(identityMod)(identity =>
              tbd.write(identity))
          else
            randomReduceList(head, identity, 0, roundMemoizer)))
    }
  }

  def filter(
      tbd: TBD,
      pred: ((T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): ModList[T, V] = {
    val memo = tbd.makeMemoizer[Mod[ModListNode[T, V]]](!memoized)

    new ModList(
      tbd.mod {
        tbd.read(head)(node => {
          if (node != null) {
            node.filter(tbd, pred, memo)
          } else {
            tbd.write[ModListNode[T, V]](null)
          }
        })
      }
    )
  }

  def split(
      tbd: TBD,
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false
    ): (AdjustableList[T, V], AdjustableList[T, V]) = {
    val memo = tbd.makeMemoizer[Changeable2[ModListNode[T, V], ModListNode[T, V]]]()

    val result = tbd.mod_2(2) {
      tbd.read(head)(head => {
	memo(head) {
	  if (head == null) {
	    tbd.write2(null.asInstanceOf[ModListNode[T, V]],
		       null.asInstanceOf[ModListNode[T, V]])
	  } else {
	    head.split(tbd, memo, pred)
	  }
	}
      })
    }

    (new ModList(result._1), new ModList(result._2))
  }

  def sort(
      tbd: TBD,
      comperator: (TBD, (T, V), (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): AdjustableList[T, V] = {

    val memo = tbd.makeMemoizer[Mod[ModListNode[T, V]]](!memoized)

    val sorted = tbd.mod {
      tbd.read(head)(head => {
        if(head == null) {
          tbd.write[ModListNode[T, V]](null)
        } else {
          head.quicksort(tbd, tbd.createMod(null),
                         comperator, memo, parallel, memoized)
        }
      })
    }

    new ModList(sorted)
  }

  def toBuffer(): Buffer[V] = {
    val buf = ArrayBuffer[V]()
    var node = head.read()
    while (node != null) {
      buf += node.value._2
      node = node.next.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
