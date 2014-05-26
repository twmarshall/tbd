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

import tbd.{Changeable, TBD}
import tbd.memo.Lift

class DoubleModListNode[T, V] (
    _value: Mod[(T, V)],
    _next: Mod[DoubleModListNode[T, V]])
    extends Iterator[T, V, DoubleModListNode[T, V]] {
  def value = _value
  def next = _next

  def map[U, Q](
      tbd: TBD,
      dest: Dest[DoubleModListNode[U, Q]],
      f: (TBD, (T, V)) => (U, Q),
      lift: Lift[Mod[DoubleModListNode[U, Q]]]
      ): Changeable[DoubleModListNode[U, Q]] = {
    val newValue = tbd.mod((dest: Dest[(U, Q)]) =>
      tbd.read(value)(value => tbd.write(dest, f(tbd, value))))
    val newNext = lift.memo(List(next), () => {
      tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) =>
        tbd.read(next)(next => {
          if (next != null) {
            next.map(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      )
    })
    tbd.write(dest, new DoubleModListNode[U, Q](newValue, newNext))
  }

  def parMap[U, Q](
      tbd: TBD,
      dest: Dest[DoubleModListNode[U, Q]],
      f: (TBD, (T, V)) => (U, Q)): Changeable[DoubleModListNode[U, Q]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	      tbd.mod((valueDest: Dest[(U, Q)]) => {
          tbd.read(value)(value => {
            tbd.write(valueDest, f(tbd, value))
          })
        })
      }, (tbd: TBD) => {
        tbd.mod((nextDest: Dest[DoubleModListNode[U, Q]]) => {
	        tbd.read(next)(next => {
            if (next != null) {
              next.parMap(tbd, nextDest, f)
            } else {
              tbd.write(nextDest, null)
            }
          })
        })
      })
    tbd.write(dest, new DoubleModListNode[U, Q](modTuple._1, modTuple._2))
  }

  def split(
      tbd: TBD,
      destMatch: Dest[DoubleModListNode[T, V]],
      destNoMatch: Dest[DoubleModListNode[T, V]],
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[DoubleModListNode[T, V]] = {
    tbd.read2(value, next)((v, next) => {
      if(pred(tbd, (v._1, v._2))) {
        val newNext = tbd.mod((newDest: Dest[DoubleModListNode[T, V]]) => {
          if(next != null) {
            next.split(tbd, newDest, destNoMatch, pred)
          } else {
            tbd.write(newDest, null)
            tbd.write(destNoMatch, null)
          }
        })

        tbd.write(destMatch, new DoubleModListNode(value, newNext))
      } else {
        val newNext = tbd.mod((newDest: Dest[DoubleModListNode[T, V]]) => {
          if(next != null) {
            next.split(tbd, destMatch, newDest, pred)
          } else {
            tbd.write(newDest, null)
            tbd.write(destMatch, null)
          }
        })

        tbd.write(destNoMatch, new DoubleModListNode(value, newNext))
      }
    })
  }

  def quicksort(
        tbd: TBD,
        dest: Dest[DoubleModListNode[T, V]],
        toAppend: Mod[DoubleModListNode[T, V]],
        comperator: (TBD, (T, V), (T, V)) => Boolean,
        parallel: Boolean = false,
        memoized: Boolean = false):
          Changeable[DoubleModListNode[T, V]] = {
    tbd.read(next)(next => {
      if(next != null) {
        tbd.read(value)(v => {
          val splitResult = tbd.mod2((matches: Dest[DoubleModListNode[T, V]],
                                      diffs: Dest[DoubleModListNode[T, V]]) => {

            next.split(tbd, matches, diffs,
              (tbd, cv) => { comperator(tbd, cv, v) },
              parallel, memoized)
          })
          tbd.read2(splitResult._1, splitResult._2)((smaller, greater) => {
            val greaterSorted = tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
              if(greater != null) {
                greater.quicksort(tbd, dest, toAppend,
                                  comperator, parallel, memoized)
              } else {
                tbd.write(dest, null)
              }
            })

            val mid = new DoubleModListNode(value, greaterSorted)

            if(smaller != null) {
              smaller.quicksort(tbd, dest, tbd.createMod(mid),
                                comperator, parallel, memoized)
            } else {
              tbd.write(dest, mid)
            }

          })
        })
      } else {
        tbd.write(dest, new DoubleModListNode(value, toAppend))
      }
    })
  }

  def halfListReduce(
      tbd: TBD,
      round: Int,
      hasher: Hasher,
      lift: Lift[Mod[DoubleModListNode[T, V]]],
      dest: Dest[DoubleModListNode[T, V]],
      f: (TBD, (T, V), (T, V)) => (T, V)): Changeable[DoubleModListNode[T, V]] = {
    tbd.read2(next, value)((next, v) => {
      if(next == null) {
            tbd.write(dest, this)
      } else {

        val reducedList = lift.memo(List(next.next, next.value), () => {
          tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
            next.halfListReduce(tbd, round, hasher, lift, dest, f)
          })
        })

        if(binaryHash(v._1, round, hasher)) {
          // Do not merge the current node with the reduced list.
          val newList =  new DoubleModListNode(value,
                                               reducedList)
          tbd.write(dest, newList)
        } else {
          // Merge the current node with the reduced list.
          tbd.read(reducedList)(reducedList => {
            tbd.read(reducedList.value)((nextValue) => {

              val reductionResult = f(tbd, nextValue, v)
              val newValue = reductionResult._2
              val newKey = reductionResult._1

              val newList = new DoubleModListNode(tbd.createMod(newKey, newValue),
                                                  reducedList.next)
              tbd.write(dest, newList)
            })
          })
        }
      }
    })
  }

  def binaryHash(id: T, round: Int, hasher: Hasher) = {
    hasher.hash(id.hashCode() ^ round) == 0
  }

  def filter(
      tbd: TBD,
      dest: Dest[DoubleModListNode[T, V]],
      pred: ((T, V)) => Boolean,
      lift: Lift[Mod[DoubleModListNode[T, V]]])
        : Changeable[DoubleModListNode[T, V]] = {
    tbd.read(value)(value => {
      if (pred(value)) {
        val newNext = lift.memo(List(next), () => {
          tbd.mod((nextDest: Dest[DoubleModListNode[T, V]]) => {
            tbd.read(next)(nextValue => {
                if (nextValue == null) {
                  tbd.write(nextDest, null)
                } else {
                  nextValue.filter(tbd, nextDest, pred, lift)
                }
              })
            })
          })
        tbd.write(dest, new DoubleModListNode(tbd.createMod(value), newNext))
      } else {
        tbd.read(next)(nextValue => {
          if (nextValue == null) {
            tbd.write(dest, null)
          } else {
            nextValue.filter(tbd, dest, pred, lift)
          }
        })
      }
    })
  }

  override def toString: String = {
    def toString(lst: DoubleModListNode[T, V]):String = {
      val nextRead = lst.next.read()
      val next =
	if (nextRead != null)
	  ", " + toString(nextRead)
	else
	  ")"

      lst.value + next
    }

    "DoubleModListNode(" + toString(this)
  }
}
