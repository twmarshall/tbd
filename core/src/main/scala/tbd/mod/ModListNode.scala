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

  def map[U, Q](
      tbd: TBD,
      f: (TBD, (T, V)) => (U, Q),
      memo: Memoizer[Changeable[ModListNode[U, Q]]])
        : Changeable[ModListNode[U, Q]] = {
    val newNext = tbd.mod {
      tbd.read(next) {
	case null =>
	  tbd.write[ModListNode[U, Q]](null)
	case next =>
          memo(next) {
            next.map(tbd, f, memo)
          }
      }
    }

    tbd.write(new ModListNode[U, Q](f(tbd, value), newNext))
  }

  def parMap[U, Q](
      tbd: TBD,
      f: (TBD, (T, V)) => (U, Q)): Changeable[ModListNode[U, Q]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	f(tbd, value)
      }, (tbd: TBD) => {
        tbd.mod {
	  tbd.read(next)(next => {
            if (next != null) {
              next.parMap(tbd, f)
            } else {
              tbd.write[ModListNode[U, Q]](null)
            }
          })
        }
      })
    tbd.write(new ModListNode[U, Q](modTuple._1, modTuple._2))
  }

  def filter(
      tbd: TBD,
      pred: ((T, V)) => Boolean,
      memo: Memoizer[Mod[ModListNode[T, V]]])
        : Changeable[ModListNode[T, V]] = {
    if (pred(value)) {
      val newNext = memo(List(next)) {
        tbd.mod {
          tbd.read(next)(nextValue => {
            if (nextValue == null) {
              tbd.write[ModListNode[T, V]](null)
            } else {
              nextValue.filter(tbd, pred, memo)
            }
          })
        }
      }
      tbd.write(new ModListNode(value, newNext))
    } else {
      tbd.read(next)(nextValue => {
        if (nextValue == null) {
          tbd.write[ModListNode[T, V]](null)
        } else {
          nextValue.filter(tbd, pred, memo)
        }
      })
    }
  }

  def split(
      tbd: TBD,
      memo: Memoizer[Changeable2[ModListNode[T, V], ModListNode[T, V]]],
      pred: (TBD, (T, V)) => Boolean,
      parallel: Boolean = false
    ): Changeable2[ModListNode[T, V], ModListNode[T, V]] = {
    def readNext(next: ModListNode[T, V]) = {
      memo(next) {
	if (next != null) {
	  next.split(tbd, memo, pred)
	} else {
	  tbd.write2(null.asInstanceOf[ModListNode[T, V]],
		     null.asInstanceOf[ModListNode[T, V]])
	}
      }
    }

    if(pred(tbd, value)) {
      val (matchNext, diffNext) =
	tbd.mod_2(0) {
	  tbd.read(next)(readNext)
	}

      tbd.writeNoDestLeft(new ModListNode(value, matchNext), diffNext)
    } else {
      val (matchNext, diffNext) =
	tbd.mod_2(1) {
	  tbd.read(next)(readNext)
	}

      tbd.writeNoDestRight(matchNext, new ModListNode(value, diffNext))
    }
  }

  def quicksort(
        tbd: TBD,
        toAppend: Mod[ModListNode[T, V]],
        comperator: (TBD, (T, V), (T, V)) => Boolean,
        memo: Memoizer[Mod[ModListNode[T, V]]],
        parallel: Boolean = false,
        memoized: Boolean = false):
          Changeable[ModListNode[T, V]] = {
    tbd.read(next)(next => {
      if(next != null) {
        val (smaller, greater) = tbd.mod_2(2) {

          val memo = tbd.makeMemoizer[Changeable2[ModListNode[T, V], ModListNode[T, V]]]()

          next.split(tbd, memo,
                     (tbd, cv) => { comperator(tbd, cv, value) },
                     parallel)
        }

        val greaterSorted = memo(List(greater)) {
          tbd.mod {
            tbd.read(greater)(greater => {
              if(greater != null) {
                greater.quicksort(tbd, toAppend,
                                  comperator, memo, parallel, memoized)
              } else {
                tbd.read(toAppend)(toAppend => {
                  tbd.write(toAppend)
                })
              }
            })
          }
        }

        val mid = new ModListNode(value, greaterSorted)

        tbd.read(smaller)(smaller => {
          if(smaller != null) {
            smaller.quicksort(tbd, tbd.createMod(mid),
                              comperator, memo, parallel, memoized)
          } else {
            tbd.write(mid)
          }
        })
      } else {
        tbd.write(new ModListNode(value, toAppend))
      }
    })
  }
}
