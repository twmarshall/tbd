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

  def fastSplit(
      tbd: TBD,
      destMatch: Dest[DoubleModListNode[T, V]],
      destNoMatch: Dest[DoubleModListNode[T, V]],
      matchLift: Lift[Mod[DoubleModListNode[T, V]]],
      diffLift: Lift[Mod[DoubleModListNode[T, V]]],
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[DoubleModListNode[T, V]] = {
    tbd.read(value)((v) => {
      if(pred(tbd, (v._1, v._2))) {
        val newDestNext = matchLift.memo(List(next), () => {
          tbd.mod((newDest: Dest[DoubleModListNode[T, V]]) => {
            tbd.read(next)(next => {
              if(next != null) {
                next.fastSplit(tbd, newDest, destNoMatch, matchLift, diffLift, pred)
              } else {
                tbd.write(newDest, null)
                tbd.write(destNoMatch, null)
              }
            })
          })
        })

        tbd.write(destMatch, new DoubleModListNode(value, newDestNext))
      } else {
        val newDiffNext = diffLift.memo(List(next), () => {
          tbd.mod((newDest: Dest[DoubleModListNode[T, V]]) => {
            tbd.read(next)(next => {
             if(next != null) {
                next.fastSplit(tbd, destMatch, newDest, matchLift, diffLift, pred)
              } else {
                tbd.write(newDest, null)
                tbd.write(destMatch, null)
              }
            })
          })
        })

        tbd.write(destNoMatch, new DoubleModListNode(value, newDiffNext))
      }
    })
  }

  def split(
      tbd: TBD,
      dest: Dest[(DoubleModListNode[T, V], DoubleModListNode[T, V])],
      pred: (TBD, (T, V)) => Boolean,
      lift: Lift[Mod[(DoubleModListNode[T, V], DoubleModListNode[T, V])]],
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[(DoubleModListNode[T, V], DoubleModListNode[T, V])] = {

    val newNext = lift.memo(List(next), () => {
      tbd.mod((dest:  Dest[(DoubleModListNode[T, V], DoubleModListNode[T, V])]) => {
        tbd.read(next)(next => {
          if(next != null) {
            next.split(tbd, dest, pred, lift, parallel, memoized)
          } else {
            tbd.write(dest, (null, null))
          }
        })
      })
    })

    tbd.read(value)((v) => {
      if(pred(tbd, (v._1, v._2))) {
        tbd.read(newNext)(newNext => {
          tbd.write(dest, (new DoubleModListNode(value, tbd.createMod(newNext._1)), newNext._2))
        })
      } else {
        tbd.read(newNext)(newNext => {
          tbd.write(dest, (newNext._1, new DoubleModListNode(value, tbd.createMod(newNext._2))))
        })
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
          val splitResult = tbd.mod((dest: Dest[(DoubleModListNode[T, V],
                                           DoubleModListNode[T, V])]) => {

            next.split(tbd, dest,
              (tbd, cv) => { comperator(tbd, cv, v) }, tbd.makeLift[Mod[(DoubleModListNode[T, V],
                                                                          DoubleModListNode[T, V])]](true),
              parallel, memoized)
          })
          tbd.read(splitResult)(splitResult => {
            val (smaller, greater) = splitResult
            val greaterSorted = tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
              if(greater != null) {
                greater.quicksort(tbd, dest, toAppend,
                                  comperator, parallel, memoized)
              } else {
                tbd.read(toAppend)(toAppend => {
                  tbd.write(dest, toAppend)
                })
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
