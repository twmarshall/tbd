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

import java.io.Serializable

import tbd.{Changeable, Changeable2, Memoizer, TBD}

class ModListNode[T, V] (
    var value: (T, V),
    val next: Mod[ModListNode[T, V]]
  ) extends Serializable {

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ModListNode[T, V]]) {
      false
    } else {
      val that = obj.asInstanceOf[ModListNode[T, V]]
      that.value == value && that.next == next
    }
  }

  def mapDest[U, Q](
      tbd: TBD,
      f: (TBD, (T, V)) => (U, Q),
      dest: Dest[ModListNode[U, Q]],
      memo: Memoizer[Changeable[ModListNode[U, Q]]],
      memo2: Memoizer[Dest[ModListNode[U, Q]]]
    ) : Changeable[ModListNode[U, Q]] = {
    val newDest = memo2(value._1) {
      tbd.createDest[ModListNode[U, Q]]()
    }
    tbd.read(next)(next => {
      if (next != null) {
        memo(next, newDest) {
          next.mapDest(tbd, f, newDest, memo, memo2)
        }
      } else {
        tbd.write(newDest, null)
      }
    })

    tbd.write(dest, new ModListNode[U, Q](f(tbd, value), newDest.mod))
  }

  def mapDest2[U, Q](
      tbd: TBD,
      f: (TBD, (T, V)) => (U, Q),
      dest: Dest[ModListNode[U, Q]],
      memo: Memoizer[Mod[ModListNode[U, Q]]]
    ) : Changeable[ModListNode[U, Q]] = {
    val newNext = memo(next) {
      tbd.mod((newDest: Dest[ModListNode[U, Q]]) => {
        tbd.read(next)(next => {
          if (next != null) {
            next.mapDest2(tbd, f, newDest, memo)
          } else {
            tbd.write(newDest, null)
          }
        })
      })
    }

    tbd.write(dest, new ModListNode[U, Q](f(tbd, value), newNext))
  }

  def parMap[U, Q](
      tbd: TBD,
      dest: Dest[ModListNode[U, Q]],
      f: (TBD, (T, V)) => (U, Q)): Changeable[ModListNode[U, Q]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	f(tbd, value)
      }, (tbd: TBD) => {
        tbd.mod((nextDest: Dest[ModListNode[U, Q]]) => {
	        tbd.read(next)(next => {
            if (next != null) {
              next.parMap(tbd, nextDest, f)
            } else {
              tbd.write(nextDest, null)
            }
          })
        })
      })
    tbd.write(dest, new ModListNode[U, Q](modTuple._1, modTuple._2))
  }

  def filter(
      tbd: TBD,
      dest: Dest[ModListNode[T, V]],
      pred: ((T, V)) => Boolean,
      memo: Memoizer[Mod[ModListNode[T, V]]])
        : Changeable[ModListNode[T, V]] = {
    if (pred(value)) {
      val newNext = memo(List(next)) {
        tbd.mod((nextDest: Dest[ModListNode[T, V]]) => {
          tbd.read(next)(nextValue => {
            if (nextValue == null) {
              tbd.write(nextDest, null)
            } else {
              nextValue.filter(tbd, nextDest, pred, memo)
            }
          })
        })
      }
      tbd.write(dest, new ModListNode(value, newNext))
    } else {
      tbd.read(next)(nextValue => {
        if (nextValue == null) {
          tbd.write(dest, null)
        } else {
          nextValue.filter(tbd, dest, pred, memo)
        }
      })
    }
  }

  def split(
      tbd: TBD,
      destMatch: Dest[ModListNode[T, V]],
      destNoMatch: Dest[ModListNode[T, V]],
      memo: Memoizer[(Mod[ModListNode[T, V]], Mod[ModListNode[T, V]])],
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[ModListNode[T, V]] = {

    val (matchNext, diffNext) = memo(List(next)) {
      tbd.mod2((newDestMatch: Dest[ModListNode[T, V]], newDestNoMatch: Dest[ModListNode[T, V]]) => {
        tbd.read(next)(next => {
          if(next != null) {
            next.split(tbd, newDestMatch, newDestNoMatch, memo, pred)
          } else {
            tbd.write(newDestMatch, null)
            tbd.write(newDestNoMatch, null)
          }
        })
      })
    }

    if(pred(tbd, (value._1, value._2))) {
      tbd.write(destMatch, new ModListNode(value, matchNext))
      tbd.read(diffNext)(diffNext => {
        tbd.write(destNoMatch, diffNext)
      })
    } else {
      tbd.write(destNoMatch, new ModListNode(value, diffNext))
      tbd.read(matchNext)(matchNext => {
        tbd.write(destMatch, matchNext)
      })
    }
  }

  def splitDest(
      tbd: TBD,
      memo: Memoizer[Changeable[ModListNode[T, V]]],
      memo2: Memoizer[Dest[ModListNode[T, V]]],
      pred: (TBD, (T, V)) => Boolean,
      dest1: Dest[ModListNode[T, V]],
      dest2: Dest[ModListNode[T, V]],
      parallel: Boolean = false,
      memoized: Boolean = false):
        Changeable[ModListNode[T, V]] = {
    if(pred(tbd, value)) {
      val newDest = memo2(value._1) {
        tbd.createDest[ModListNode[T, V]]()
      }
      memo(List(next, newDest, dest2)) {
	tbd.read(next)(next => {
	  if(next != null) {
	    next.splitDest(tbd, memo, memo2, pred, newDest, dest2)
	  } else {
            tbd.write(newDest, null)
            tbd.write(dest2, null)
	  }
        })
      }

      tbd.write(dest1, new ModListNode(value, newDest.mod))
    } else {
      val newDest = memo2(value._1) {
        tbd.createDest[ModListNode[T, V]]()
      }
      memo(List(next, dest1, newDest)) {
	tbd.read(next)(next => {
	  if(next != null) {
	    next.splitDest(tbd, memo, memo2, pred, dest1, newDest)
	  } else {
            tbd.write(dest1, null)
            tbd.write(newDest, null)
	  }
	})
      }

      tbd.write(dest2, new ModListNode(value, newDest.mod))
    }
  }

  def quicksort(
        tbd: TBD,
        dest: Dest[ModListNode[T, V]],
        toAppend: Mod[ModListNode[T, V]],
        comperator: (TBD, (T, V), (T, V)) => Boolean,
        memo: Memoizer[Mod[ModListNode[T, V]]],
        parallel: Boolean = false,
        memoized: Boolean = false):
          Changeable[ModListNode[T, V]] = {
    tbd.read(next)(next => {
      if(next != null) {
        val (smaller, greater) = tbd.mod2((destSmaller: Dest[ModListNode[T, V]],
                                           destGreater: Dest[ModListNode[T, V]]) => {

          val memo = tbd.makeMemoizer[(Mod[ModListNode[T, V]],
                                       Mod[ModListNode[T, V]])](!memoized)

          next.split(tbd, destSmaller, destGreater, memo,
                     (tbd, cv) => { comperator(tbd, cv, value) },
                     parallel, memoized)
        })

        val greaterSorted = memo(List(greater)) {
          tbd.mod((dest: Dest[ModListNode[T, V]]) => {
            tbd.read(greater)(greater => {
              if(greater != null) {
                greater.quicksort(tbd, dest, toAppend,
                                  comperator, memo, parallel, memoized)
              } else {
                tbd.read(toAppend)(toAppend => {
                  tbd.write(dest, toAppend)
                })
              }
            })
          })
        }

        val mid = new ModListNode(value, greaterSorted)

        tbd.read(smaller)(smaller => {
          if(smaller != null) {
            smaller.quicksort(tbd, dest, tbd.createMod(mid),
                              comperator, memo, parallel, memoized)
          } else {
            tbd.write(dest, mid)
          }
        })
      } else {
        tbd.write(dest, new ModListNode(value, toAppend))
      }
    })
  }
}
