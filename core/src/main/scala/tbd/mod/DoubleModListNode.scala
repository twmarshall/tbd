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
import tbd.memo.{Lift, CurriedLift}

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
      lift: Lift[(Mod[(U, Q)], Mod[DoubleModListNode[U, Q]])]
      ): Changeable[DoubleModListNode[U, Q]] = {
    val pair = lift.memo(List(next), () => {
      (tbd.mod((dest: Dest[(U, Q)]) =>
	tbd.read(value)(value => tbd.write(dest, f(tbd, value)))),
       tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) =>
        tbd.read(next)(next => {
          if (next != null) {
            next.map(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      ))
    })
    tbd.write(dest, new DoubleModListNode[U, Q](pair._1, pair._2))
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

  /*
   * Splecial split implementation for quicksort - not optimal for a single split
   * execution but can exploit memorization accross mutliple calls.
   */
  def qsplit(
      tbd: TBD,
      dest: Dest[(Mod[DoubleModListNode[T, V]], Mod[DoubleModListNode[T, V]])],
      lift: Lift[Mod[(Mod[DoubleModListNode[T, V]], Mod[DoubleModListNode[T, V]])]],
      creationLift: Lift[Mod[DoubleModListNode[T, V]]],
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[(Mod[DoubleModListNode[T, V]], Mod[DoubleModListNode[T, V]])] = {

    val res = lift.memo(List(next), () => {
      tbd.mod((dest: Dest[(Mod[DoubleModListNode[T, V]], Mod[DoubleModListNode[T, V]])]) => {
        tbd.read(next)(n => {
          if(n != null) {
            n.qsplit(tbd, dest, lift, creationLift, pred, parallel, memoized)
          } else {
            val endTuple =
              (creationLift.memo(List(next), () => {
                tbd.createMod(null)
              }),
              creationLift.memo(List(next), () => {
                tbd.createMod(null)
              }))
            tbd.write(dest, endTuple)
          }
        })
      })
    })

    tbd.read(res)(res => {

      val (matchNext, diffNext) = res

      tbd.read(value)((v) => {
        if(pred(tbd, (v._1, v._2))) {
          val newMatch = creationLift.memo(List(matchNext, value), () => {
            tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
              tbd.write(dest, new DoubleModListNode(value, matchNext))
            })
          })
          tbd.write(dest, (newMatch, diffNext))
        } else {
          val newDiff = creationLift.memo(List(diffNext, value), () => {
            tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
              tbd.write(dest, new DoubleModListNode(value, diffNext))
            })
          })
          tbd.write(dest, (matchNext, newDiff))
        }
      })
    })
  }

  def split(
      tbd: TBD,
      destMatch: Dest[DoubleModListNode[T, V]],
      destNoMatch: Dest[DoubleModListNode[T, V]],
      lift: Lift[(Mod[DoubleModListNode[T, V]], Mod[DoubleModListNode[T, V]])],
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[DoubleModListNode[T, V]] = {

    val (matchNext, diffNext) = lift.memo(List(next), () => {
      tbd.mod2((newDestMatch: Dest[DoubleModListNode[T, V]], newDestNoMatch: Dest[DoubleModListNode[T, V]]) => {
        tbd.read(next)(next => {
          if(next != null) {
            next.split(tbd, newDestMatch, newDestNoMatch, lift, pred, parallel, memoized)
          } else {
            tbd.write(newDestMatch, null)
            tbd.write(newDestNoMatch, null)
          }
        })
      })
    })

    tbd.read(value)((v) => {
      if(pred(tbd, (v._1, v._2))) {
        tbd.write(destMatch, new DoubleModListNode(value, matchNext))
        tbd.read(diffNext)(diffNext => {
          tbd.write(destNoMatch, diffNext)
        })
      } else {
        tbd.write(destNoMatch, new DoubleModListNode(value, diffNext))
        tbd.read(matchNext)(matchNext => {
          tbd.write(destMatch, matchNext)
        })
      }
    })
  }

  def quicksort(
      tbd: TBD,
      dest: Dest[DoubleModListNode[T, V]],
      toAppend: Mod[DoubleModListNode[T, V]],
      comperator: (TBD, (T, V), (T, V)) => Boolean,
      lift: Lift[Mod[DoubleModListNode[T, V]]],
      creationLift: Lift[Mod[DoubleModListNode[T, V]]],
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[DoubleModListNode[T, V]] = {
    tbd.read(next)(next => {
      if(next != null) {
        tbd.read(value)(v => {
          val res = tbd.mod((dest: Dest[(Mod[DoubleModListNode[T, V]], Mod[DoubleModListNode[T, V]])]) => {

            val splitLift = tbd.makeLift[Mod[(Mod[DoubleModListNode[T, V]],
                                  Mod[DoubleModListNode[T, V]])]](!memoized)

            next.qsplit(tbd, dest, splitLift, creationLift,
              (tbd, cv) => { comperator(tbd, cv, v) },
              parallel, memoized)
          })

          tbd.read(res)(res => {
            val (smaller, greater) = res
            val greaterSorted = tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
              tbd.read(greater)(greater => {
                if(greater != null) {
                  greater.quicksort(tbd, dest, toAppend,
                                    comperator, lift, creationLift, parallel, memoized)
                } else {
                  tbd.read(toAppend)(toAppend => {
                    tbd.write(dest, toAppend)
                  })
                }
              })
            })

            val mid = new DoubleModListNode(value, greaterSorted)
            //Maybe introduce a memo here (need new dest)?
            //Test if better perf if memo omitted for split?
            tbd.read(smaller)(smaller => {
              if(smaller != null) {
                smaller.quicksort(tbd, dest, tbd.createMod(mid),
                                  comperator, lift, creationLift, parallel, memoized)
              } else {
                tbd.write(dest, mid)
              }
            })
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
