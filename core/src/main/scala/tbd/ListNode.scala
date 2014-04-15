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
package tbd

import tbd.memo.Lift
import tbd.mod.Mod

class ListNode[T](aValue: Mod[T], aNext: Mod[ListNode[T]]) {
  val value = aValue
  val next = aNext

  def map[W](
      tbd: TBD,
      dest: Dest[ListNode[W]],
      f: T => W): Changeable[ListNode[W]] = {
    val newValue = tbd.mod((dest: Dest[W]) =>
      tbd.read(value, (value: T) => tbd.write(dest, f(value))))
    val newNext = tbd.mod((dest: Dest[ListNode[W]]) =>
      tbd.read(next, (next: ListNode[T]) => {
        if (next != null) {
          next.map(tbd, dest, f)
        } else {
          tbd.write(dest, null)
        }
      }))
    tbd.write(dest, new ListNode[W](newValue, newNext))
  }

  def memoMap[W](
      tbd: TBD,
      dest: Dest[ListNode[W]],
      f: T => W,
      lift: Lift[ListNode[T], ListNode[W]]): Changeable[ListNode[W]] = {
    val newValue = tbd.mod((dest: Dest[W]) =>
      tbd.read(value, (value: T) => tbd.write(dest, f(value))))
    val newNext = tbd.mod((dest: Dest[ListNode[W]]) =>
      lift.memo(List(next), () => {
        tbd.read(next, (next: ListNode[T]) => {
          if (next != null) {
            next.memoMap(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      }))
    tbd.write(dest, new ListNode[W](newValue, newNext))
  }

  def parMap[W](
      tbd: TBD,
      dest: Dest[ListNode[W]],
      f: (TBD, T) => W): Changeable[ListNode[W]] = {
	  val modTuple =
      tbd.par((tbd: TBD) => {
	      tbd.mod((valueDest: Dest[W]) => {
          tbd.read(value, (value: T) => {
            tbd.write(valueDest, f(tbd, value))
          })
        })
	    }, (tbd: TBD) => {
        tbd.mod((nextDest: Dest[ListNode[W]]) => {
	        tbd.read(next, (next: ListNode[T]) => {
            if (next != null) {
              next.parMap(tbd, nextDest, f)
            } else {
              tbd.write(nextDest, null)
            }
          })
        })
	    })
    tbd.write(dest, new ListNode[W](modTuple._1, modTuple._2))
  }

  def reduce(
      tbd: TBD,
      dest: Dest[ListNode[T]],
      func: (T, T) => T): Changeable[ListNode[T]] = {
    tbd.read(next, (next: ListNode[T]) => {
      if (next == null) {
        val newValue = tbd.mod((dest: Dest[T]) => {
          tbd.read(value, (value: T) => tbd.write(dest, value))
        })
        val newNext = tbd.mod((dest: Dest[ListNode[T]]) => {
          tbd.write(dest, null)
        })
        tbd.write(dest, new ListNode(newValue, newNext))
      } else {
        val newValue = tbd.mod((dest: Dest[T]) => {
          tbd.read(value, (value1: T) => {
            tbd.read(next.value, (value2: T) => {
              tbd.write(dest, func(value1, value2))
            })
          })
        })

        val newNext = tbd.mod((dest: Dest[ListNode[T]]) => {
          tbd.read(next.next, (lst: ListNode[T]) => {
            if (lst != null) {
              lst.reduce(tbd, dest, func)
            } else {
              tbd.write(dest, null)
            }
          })
        })
        tbd.write(dest, new ListNode(newValue, newNext))
      }
    })
  }

  def parReduce(
      tbd: TBD,
      dest: Dest[ListNode[T]],
      func: (TBD, T, T) => T): Changeable[ListNode[T]] = {
    tbd.read(next, (next: ListNode[T]) => {
      if (next == null) {
        val newValue = tbd.mod((dest: Dest[T]) => {
          tbd.read(value, (value: T) => tbd.write(dest, value))
        })

        val newNext = tbd.mod((dest: Dest[ListNode[T]]) => tbd.write(dest, null))
        tbd.write(dest, new ListNode(newValue, newNext))
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
          tbd.mod((dest: Dest[ListNode[T]]) => {
            tbd.read(next.next, (lst: ListNode[T]) => {
	            if (lst == null) {
	              tbd.write(dest, null)
	            } else {
		            lst.parReduce(tbd, dest, func)
	            }
            })
          })
        })

        tbd.write(dest, new ListNode(modTuple._1, modTuple._2))
      }
    })
  }

  override def toString: String = {
    def toString(lst: ListNode[T]):String = {
      val nextRead = lst.next.read()
      val next =
	if (nextRead != null)
	  ", " + toString(nextRead)
	else
	  ")"

      lst.value + next
    }

    "ListNode(" + toString(this)
  }
}
