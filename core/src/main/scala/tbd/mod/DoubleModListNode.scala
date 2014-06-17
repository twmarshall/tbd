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
      f: (TBD, (T, V)) => (U, Q),
      lift: Lift[(Mod[(U, Q)], Mod[DoubleModListNode[U, Q]])])
        : Changeable[DoubleModListNode[U, Q]] = {
    val pair = lift.memo(List(next), () => {
      (tbd.modNoDest(() =>
	tbd.read(value)(value => tbd.writeNoDest(f(tbd, value)))),
       tbd.modNoDest(() =>
         tbd.read(next)(next => {
           if (next != null) {
               next.map(tbd, f, lift)
           } else {
             tbd.writeNoDest[DoubleModListNode[U, Q]](null)
           }
         })
      ))
    })

    tbd.writeNoDest(new DoubleModListNode[U, Q](pair._1, pair._2))
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
      lift: Lift[(Mod[DoubleModListNode[T, V]], Mod[DoubleModListNode[T, V]])],
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[DoubleModListNode[T, V]] = {


    val (matchNext, diffNext) = lift.memo(List(next), () => {
      tbd.mod2((newDestMatch: Dest[DoubleModListNode[T, V]], newDestNoMatch: Dest[DoubleModListNode[T, V]]) => {
        tbd.read(next)(n => {
          if(n != null) {
            n.split(tbd, newDestMatch, newDestNoMatch, lift, pred, parallel, memoized)
          } else {
            tbd.write(newDestMatch, null)
            tbd.write(newDestNoMatch, null)
          }
        })
      })
    })

    //Note: Split behavior in this implementation is input dependant -
    //Change propagation continues until two predecessors are in different result lists.
    //This is due to the copying of values...
    tbd.read(value)((v) => {
      if(pred(tbd, (v._1, v._2))) {
        tbd.write(destMatch, new DoubleModListNode(value, matchNext))
        tbd.read(diffNext)(diffNext => { //... here and ...
            tbd.write(destNoMatch, diffNext)
        })
      } else {
        tbd.write(destNoMatch, new DoubleModListNode(value, diffNext))
        tbd.read(matchNext)(matchNext => { // ... here.
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
      akkuLift: Lift[Changeable[DoubleModListNode[T, V]]],
      stdLift: Lift[Mod[DoubleModListNode[T, V]]],
      splitLift: Lift[(Mod[DoubleModListNode[T, V]],
                       Mod[DoubleModListNode[T, V]])],
      thisMod: Mod[DoubleModListNode[T, V]],
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[DoubleModListNode[T, V]] = {
    tbd.read(next)(n => {
      if(n != null) {
        val (smaller, greater) =
          tbd.mod2((destGreater: Dest[DoubleModListNode[T, V]],
                    destSmaller: Dest[DoubleModListNode[T, V]]) => {
          tbd.read(value)(v => {
            val curriedLift = new CurriedLift(splitLift, List(value)) //Depends on value, must be initialized here.
            n.split(tbd, destGreater, destSmaller, curriedLift,
              (tbd, cv) => { comperator(tbd, cv, v) },
              parallel, memoized)
          })
        })

        val midMod = stdLift.memo(List(greater), () => {
          tbd.mod((dest: Dest[DoubleModListNode[T,V]]) => {
            val greaterSorted = tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
              tbd.read(greater)(g => {
                if(g != null) {
                  akkuLift.memo(List(g.value, g.next, dest), () => {
                    g.quicksort(tbd, dest, toAppend, comperator, akkuLift, stdLift,
                                splitLift, greater, parallel, memoized)
                  })
                } else {
                  tbd.read(toAppend)(toAppend => {
                    tbd.write(dest, toAppend)
                  })
                }
              })
            })
            tbd.write(dest, new DoubleModListNode(value, greaterSorted))
          })
        })

        tbd.read(smaller)(s => {
          if(s != null) {
            akkuLift.memo(List(s.value, s.next, dest, midMod), () => {
              s.quicksort(tbd, dest, midMod, comperator, akkuLift, stdLift,
                          splitLift, smaller, parallel, memoized)
            })
          } else {
            tbd.read(midMod)(mid => {
              akkuLift.memo(List(mid.next, mid.value, dest), () => {
                tbd.write(dest, mid)
              })
            })
          }
        })
      } else {
        tbd.write(dest, new DoubleModListNode(value, toAppend))
      }
    })
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
