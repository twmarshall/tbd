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

class DoubleModList[T](aValue: Mod[T], aNext: Mod[DoubleModList[T]]) {
  val value = aValue
  val next = aNext

  def map[W](
      tbd: TBD,
      dest: Dest[DoubleModList[W]],
      f: T => W): Changeable[DoubleModList[W]] = {
    val newValue = tbd.mod((dest: Dest[W]) =>
      tbd.read(value, (value: T) => tbd.write(dest, f(value))))
    val newNext = tbd.mod((dest: Dest[DoubleModList[W]]) =>
      tbd.read(next, (next: DoubleModList[T]) => {
        if (next != null) {
          next.map(tbd, dest, f)
        } else {
          tbd.write(dest, null)
        }
      }))
    tbd.write(dest, new DoubleModList[W](newValue, newNext))
  }

  def memoMap[W](
      tbd: TBD,
      dest: Dest[DoubleModList[W]],
      f: T => W,
      lift: Lift[DoubleModList[T], Mod[DoubleModList[W]]]): Changeable[DoubleModList[W]] = {
    val newValue = tbd.mod((dest: Dest[W]) =>
      tbd.read(value, (value: T) => tbd.write(dest, f(value))))
    val newNext = lift.memo(List(next), () => {
      tbd.mod((dest: Dest[DoubleModList[W]]) =>
        tbd.read(next, (next: DoubleModList[T]) => {
          if (next != null) {
            next.memoMap(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      )
    })
    tbd.write(dest, new DoubleModList[W](newValue, newNext))
  }

  def parMap[W](
      tbd: TBD,
      dest: Dest[DoubleModList[W]],
      f: (TBD, T) => W): Changeable[DoubleModList[W]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	tbd.mod((valueDest: Dest[W]) => {
          tbd.read(value, (value: T) => {
            tbd.write(valueDest, f(tbd, value))
          })
        })
      }, (tbd: TBD) => {
        tbd.mod((nextDest: Dest[DoubleModList[W]]) => {
	  tbd.read(next, (next: DoubleModList[T]) => {
            if (next != null) {
              next.parMap(tbd, nextDest, f)
            } else {
              tbd.write(nextDest, null)
            }
            })
        })
      })

    tbd.write(dest, new DoubleModList[W](modTuple._1, modTuple._2))
  }

  def reduce(
      tbd: TBD,
      dest: Dest[DoubleModList[T]],
      func: (T, T) => T): Changeable[DoubleModList[T]] = {
    tbd.read(next, (next: DoubleModList[T]) => {
      if (next == null) {
        val newValue = tbd.mod((dest: Dest[T]) => {
          tbd.read(value, (value: T) => tbd.write(dest, value))
        })
        val newNext = tbd.mod((dest: Dest[DoubleModList[T]]) => {
          tbd.write(dest, null)
        })
        tbd.write(dest, new DoubleModList(newValue, newNext))
      } else {
        val newValue = tbd.mod((dest: Dest[T]) => {
          tbd.read(value, (value1: T) => {
            tbd.read(next.value, (value2: T) => {
              tbd.write(dest, func(value1, value2))
            })
          })
        })

        val newNext = tbd.mod((dest: Dest[DoubleModList[T]]) => {
          tbd.read(next.next, (lst: DoubleModList[T]) => {
            if (lst != null) {
              lst.reduce(tbd, dest, func)
            } else {
              tbd.write(dest, null)
            }
          })
        })
        tbd.write(dest, new DoubleModList(newValue, newNext))
      }
    })
  }

  def parReduce(
      tbd: TBD,
      dest: Dest[DoubleModList[T]],
      func: (TBD, T, T) => T): Changeable[DoubleModList[T]] = {
    tbd.read(next, (next: DoubleModList[T]) => {
      if (next == null) {
        val newValue = tbd.mod((dest: Dest[T]) => {
          tbd.read(value, (value: T) => tbd.write(dest, value))
        })

        val newNext = tbd.mod((dest: Dest[DoubleModList[T]]) => tbd.write(dest, null))
        tbd.write(dest, new DoubleModList(newValue, newNext))
      } else {
        val modTuple = tbd.par((tbd:TBD) => {
          tbd.mod((dest: Dest[T]) => {
            tbd.read(value, (value1: T) => {
              tbd.read(next.value, (value2: T) => {
                tbd.write(dest, func(tbd, value1, value2))
              })
            })
          })
        }, (tbd:TBD) => {
          tbd.mod((dest: Dest[DoubleModList[T]]) => {
            tbd.read(next.next, (lst: DoubleModList[T]) => {
	            if (lst == null) {
	              tbd.write(dest, null)
	            } else {
		            lst.parReduce(tbd, dest, func)
	            }
            })
          })
        })

        tbd.write(dest, new DoubleModList(modTuple._1, modTuple._2))
      }
    })
  }

  override def toString: String = {
    def toString(lst: DoubleModList[T]):String = {
      val nextRead = lst.next.read()
      val next =
	if (nextRead != null)
	  ", " + toString(nextRead)
	else
	  ")"

      lst.value + next
    }

    "DoubleModList(" + toString(this)
  }
}
