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

import tbd.{Changeable, Memoizer, TBD}

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
    val newNext = tbd.modNoDest[ModListNode[U, Q]](() => {
      tbd.read(next)(next => {
        if (next != null) {
          memo(next) {
            next.map(tbd, f, memo)
          }
        } else {
          tbd.writeNoDest[ModListNode[U, Q]](null)
        }
      })
    })

    tbd.writeNoDest(new ModListNode[U, Q](f(tbd, value), newNext))
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
}
