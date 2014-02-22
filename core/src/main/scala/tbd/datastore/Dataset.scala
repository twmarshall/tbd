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
package tbd.datastore

import tbd.{Changeable, Dest, ListNode, TBD}
import tbd.mod.Mod

class Dataset[T](aList: Mod[ListNode[T]]) {
  val list = aList

  def map[U](tbd: TBD, func: T => U): Dataset[U] = {
    def innerMap(dest: Dest[ListNode[U]], lst: ListNode[T]):
        Changeable[ListNode[U]] = {
      if (lst != null) {
        val newValue = tbd.mod((dest: Dest[U]) =>
          tbd.read(lst.value, (value: T) => tbd.write(dest, func(value))))
        val newNext = tbd.mod((dest: Dest[ListNode[U]]) =>
          tbd.read(lst.next, (next: ListNode[T]) => innerMap(dest, next)))
        tbd.write(dest, new ListNode[U](newValue, newNext))
      } else {
        tbd.write(dest, null)
      }
    }

    new Dataset(tbd.mod((dest) => {
      tbd.read(list, (lst: ListNode[T]) => innerMap(dest, lst))
    }))
  }

  def parMap[U](tbd: TBD, func: T => U): Dataset[U] = {
    def innerMap(tbd: TBD, dest: Dest[ListNode[U]], lst: ListNode[T]):
        Changeable[ListNode[U]] = {
      if (lst != null) {
	      val modTuple =
          tbd.par((tbd: TBD) => {
	          tbd.mod((valueDest: Dest[U]) => {
              tbd.read(lst.value, (value: T) => {
                tbd.write(valueDest, func(value))
              })
            })
	        }, (tbd: TBD) => {
            tbd.mod((nextDest: Dest[ListNode[U]]) => {
	            tbd.read(lst.next, (next: ListNode[T]) => {
                innerMap(tbd, nextDest, next)
              })
            })
	        })
        tbd.write(dest, new ListNode[U](modTuple._1, modTuple._2))
      } else {
        tbd.write(dest, null)
      }
    }
    new Dataset(tbd.mod((dest) => {
      tbd.read(list, (lst: ListNode[T]) => innerMap(tbd, dest, lst))
    }))
  }



  private def reduceHelper(
      tbd: TBD,
      dest: Dest[ListNode[T]],
      lst: ListNode[T],
      func: (T, T) => T): Changeable[ListNode[T]] = {
    if (lst == null) {
      tbd.write(dest, null)
    } else {
      tbd.read(lst.next, (next: ListNode[T]) => {
        if (next == null) {
          val newValue = tbd.mod((dest: Dest[T]) => {
            tbd.read(lst.value, (value: T) => tbd.write(dest, value))
          })
          val newNext = tbd.mod((dest: Dest[ListNode[T]]) => {
            tbd.write(dest, null)
          })
          tbd.write(dest, new ListNode(newValue, newNext))
        } else {
          val newValue = tbd.mod((dest: Dest[T]) => {
            tbd.read(lst.value, (value1: T) => {
              tbd.read(next.value, (value2: T) => {
                tbd.write(dest, func(value1, value2))
              })
            })
          })

          val newNext = tbd.mod((dest: Dest[ListNode[T]]) => {
            tbd.read(next.next, (lst: ListNode[T]) => {
              reduceHelper(tbd, dest, lst, func)
            })
          })
          tbd.write(dest, new ListNode(newValue, newNext))
        }
      })
    }
  }

  def reduce(tbd: TBD, func: (T, T) => (T)): Mod[T] = {
    def innerReduce(dest: Dest[T], lst: ListNode[T]): Changeable[T] = {
      if (lst != null) {
        tbd.read(lst.next, (next: ListNode[T]) => {
          if (next != null) {
            val newList = tbd.mod((dest: Dest[ListNode[T]]) => {
              reduceHelper(tbd, dest, lst, func)
            })

            tbd.read(newList, (lst: ListNode[T]) => innerReduce(dest, lst))
          } else {
            tbd.read(lst.value, (value: T) => tbd.write(dest, value))
          }
        })
      } else {
        tbd.write(dest, null.asInstanceOf[T])
      }
    }
    tbd.mod((dest) => {
      tbd.read(list, (lst: ListNode[T]) => innerReduce(dest, lst))
    })
  }

  private def parReduceHelper(
      tbd: TBD,
      dest: Dest[ListNode[T]],
      lst: ListNode[T],
      func: (T, T) => T,
      next: ListNode[T]): Changeable[ListNode[T]] = {
    if (next == null) {
      val newValue = tbd.mod((dest: Dest[T]) => {
        tbd.read(lst.value, (value: T) => tbd.write(dest, value))            
      })
      val newNext = tbd.mod((dest: Dest[ListNode[T]]) => tbd.write(dest, null))
      tbd.write(dest, new ListNode(newValue, newNext))
    } else {
      val modTuple = tbd.par((tbd:TBD) => {
        tbd.mod((dest: Dest[T]) => {
          tbd.read(lst.value, (value1: T) => {
            tbd.read(next.value, (value2: T) => {
              tbd.write(dest, func(value1, value2))
            })
          })
        })
      }, (tbd:TBD) => {
        tbd.mod((dest: Dest[ListNode[T]]) => {
          tbd.read(next.next, (lst: ListNode[T]) => {
	          if (lst == null) {
	            tbd.write(dest, null)
	          } else {
	            tbd.read(lst.next, (next: ListNode[T]) => {
		            parReduceHelper(tbd, dest, lst, func, next)
	            })
	          }
          })
        })
      })

      tbd.write(dest, new ListNode(modTuple._1, modTuple._2))
    }
  }

  def parReduce(tbd: TBD, func: (T, T) => T): Mod[T] = {
    def innerReduce(tbd: TBD, dest: Dest[T], lst: ListNode[T]):
        Changeable[T] = {
      if (lst != null) {
        tbd.read(lst.next, (next: ListNode[T]) => {
          if (next != null) {
            val newList = tbd.mod((dest: Dest[ListNode[T]]) => {
	            parReduceHelper(tbd, dest, lst, func, next)
	          })
            tbd.read(newList, (lst: ListNode[T]) => innerReduce(tbd, dest, lst))
          } else {
            tbd.read(lst.value, (value: T) => tbd.write(dest, value))
          }
        })
      } else {
        tbd.write(dest, null.asInstanceOf[T])
      }
    }
    tbd.mod((dest) => {
      tbd.read(list, (lst: ListNode[T]) => innerReduce(tbd, dest, lst))
    })
  }
}
