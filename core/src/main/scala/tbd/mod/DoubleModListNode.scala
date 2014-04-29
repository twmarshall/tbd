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

class DoubleModListNode[T](
    aValue: Mod[T],
    aNext: Mod[DoubleModListNode[T]]) {
  val value = aValue
  val next = aNext

  def map[U](
      tbd: TBD,
      dest: Dest[DoubleModListNode[U]],
      f: T => U): Changeable[DoubleModListNode[U]] = {
    val newValue = tbd.mod((dest: Dest[U]) =>
      tbd.read(value, (value: T) => tbd.write(dest, f(value))))
    val newNext = tbd.mod((dest: Dest[DoubleModListNode[U]]) =>
      tbd.read(next, (next: DoubleModListNode[T]) => {
        if (next != null) {
          next.map(tbd, dest, f)
        } else {
          tbd.write(dest, null)
        }
      }))
    tbd.write(dest, new DoubleModListNode[U](newValue, newNext))
  }

  def memoMap[W](
      tbd: TBD,
      dest: Dest[DoubleModListNode[W]],
      f: T => W,
      lift: Lift[DoubleModListNode[T], Mod[DoubleModListNode[W]]]): Changeable[DoubleModListNode[W]] = {
    val newValue = tbd.mod((dest: Dest[W]) =>
      tbd.read(value, (value: T) => tbd.write(dest, f(value))))
    val newNext = lift.memo(List(next), () => {
      tbd.mod((dest: Dest[DoubleModListNode[W]]) =>
        tbd.read(next, (next: DoubleModListNode[T]) => {
          if (next != null) {
            next.memoMap(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      )
    })
    tbd.write(dest, new DoubleModListNode[W](newValue, newNext))
  }

  def parMap[W](
      tbd: TBD,
      dest: Dest[DoubleModListNode[W]],
      f: (TBD, T) => W): Changeable[DoubleModListNode[W]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	tbd.mod((valueDest: Dest[W]) => {
          tbd.read(value, (value: T) => {
            tbd.write(valueDest, f(tbd, value))
          })
        })
      }, (tbd: TBD) => {
        tbd.mod((nextDest: Dest[DoubleModListNode[W]]) => {
	  tbd.read(next, (next: DoubleModListNode[T]) => {
            if (next != null) {
              next.parMap(tbd, nextDest, f)
            } else {
              tbd.write(nextDest, null)
            }
          })
        })
      })

    tbd.write(dest, new DoubleModListNode[W](modTuple._1, modTuple._2))
  }

  override def toString: String = {
    def toString(lst: DoubleModListNode[T]):String = {
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
