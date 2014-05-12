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

class DoubleModListNode[T, V](
    aValue: Mod[T],
    aKey: V,
    aNext: Mod[DoubleModListNode[T, V]]) {
  val valueMod = aValue
  val next = aNext
  val key = aKey

  def map[U, Q](
      tbd: TBD,
      dest: Dest[DoubleModListNode[U, Q]],
      f: (TBD, T, V) => (U, Q),
      lift: Lift[Mod[DoubleModListNode[U, Q]]]
      ): Changeable[DoubleModListNode[U, Q]] = {
    val newValue = tbd.mod((dest: Dest[(U, Q)]) =>
      tbd.read(valueMod)(value => tbd.write(dest, f(tbd, value, key))))
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
    tbd.read(newValue)(newValue => {
      tbd.write(dest, new DoubleModListNode[U, Q](
                            tbd.createMod(newValue._1), 
                            newValue._2, 
                            newNext))
    })
  }

  def parMap[U, Q](
      tbd: TBD,
      dest: Dest[DoubleModListNode[U, Q]],
      f: (TBD, T, V) => (U, Q)): Changeable[DoubleModListNode[U, Q]] = {
    val modTuple =
      tbd.par((tbd: TBD) => {
	      tbd.mod((valueDest: Dest[(U, Q)]) => {
          tbd.read(valueMod)(value => {
            tbd.write(valueDest, f(tbd, value, key))
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
    tbd.read(modTuple._1)(value => 
      tbd.write(dest, new DoubleModListNode[U, Q](
                            tbd.createMod(value._1), 
                            value._2, 
                            modTuple._2)))
  }

  def filter(
      tbd: TBD,
      dest: Dest[DoubleModListNode[T, V]],
      pred: (T, V) => Boolean,
      lift: Lift[Mod[DoubleModListNode[T, V]]])
        : Changeable[DoubleModListNode[T, V]] = {
    tbd.read(valueMod)(value => {
      if (pred(value, key)) {
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
        tbd.write(dest, new DoubleModListNode(valueMod, key, newNext))
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

      lst.valueMod + next
    }

    "DoubleModListNode(" + toString(this)
  }
}
