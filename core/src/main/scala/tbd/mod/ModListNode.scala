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
import tbd.TBD._

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
      f: (TBD, (T, V)) => (U, Q),
      memo: Memoizer[Changeable[ModListNode[U, Q]]])
     (implicit tbd: TBD): Changeable[ModListNode[U, Q]] = {
    val newNext = mod {
      read(next) {
	case null =>
	  write[ModListNode[U, Q]](null)
	case next =>
          memo(next) {
            next.map(f, memo)
          }
      }
    }

    write(new ModListNode[U, Q](f(tbd, value), newNext))
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
      pred: ((T, V)) => Boolean,
      memo: Memoizer[Mod[ModListNode[T, V]]])
     (implicit tbd: TBD): Changeable[ModListNode[T, V]] = {
    def readNext = {
      read(next) {
	case null => write[ModListNode[T, V]](null)
	case next => next.filter(pred, memo)
      }
    }

    if (pred(value)) {
      val newNext = memo(List(next)) {
        mod {
	  readNext
        }
      }
      write(new ModListNode(value, newNext))
    } else {
      readNext
    }
  }

  def split(
      memo: Memoizer[Changeable2[ModListNode[T, V], ModListNode[T, V]]],
      pred: (TBD, (T, V)) => Boolean)
     (implicit tbd: TBD): Changeable2[ModListNode[T, V], ModListNode[T, V]] = {
    def readNext(next: ModListNode[T, V]) = {
      memo(next) {
	if (next != null) {
	  next.split(memo, pred)
	} else {
	  write2(null.asInstanceOf[ModListNode[T, V]],
		 null.asInstanceOf[ModListNode[T, V]])
	}
      }
    }

    if(pred(tbd, value)) {
      val (matchNext, diffNext) =
	mod2(0) {
	  read(next)(readNext)
	}

      tbd.writeNoDestLeft(new ModListNode(value, matchNext), diffNext)
    } else {
      val (matchNext, diffNext) =
	mod2(1) {
	  read(next)(readNext)
	}

      tbd.writeNoDestRight(matchNext, new ModListNode(value, diffNext))
    }
  }

  def quicksort(
        toAppend: Mod[ModListNode[T, V]],
        comperator: (TBD, (T, V), (T, V)) => Boolean,
        memo: Memoizer[Mod[ModListNode[T, V]]])
       (implicit tbd: TBD): Changeable[ModListNode[T, V]] = {
    read(next)(next => {
      if(next != null) {
        val (smaller, greater) = mod2(2) {

          val memo = makeMemoizer[Changeable2[ModListNode[T, V], ModListNode[T, V]]]()

          next.split(memo, (tbd, cv) => { comperator(tbd, cv, value) })
        }

        val greaterSorted = memo(List(greater)) {
          mod {
            read(greater)(greater => {
              if(greater != null) {
                greater.quicksort(toAppend, comperator, memo)
              } else {
                read(toAppend)(toAppend => {
                  write(toAppend)
                })
              }
            })
          }
        }

        val mid = new ModListNode(value, greaterSorted)

        read(smaller)(smaller => {
          if(smaller != null) {
            smaller.quicksort(createMod(mid), comperator, memo)
          } else {
            write(mid)
          }
        })
      } else {
        write(new ModListNode(value, toAppend))
      }
    })
  }
}
