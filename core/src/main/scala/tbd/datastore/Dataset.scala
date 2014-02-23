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

class Dataset[T](aLists: Mod[ListNode[Mod[ListNode[T]]]]) {
  val lists = aLists

  def map[U](tbd: TBD, func: T => U): Dataset[U] = {
    new Dataset(tbd.mod((dest: Dest[ListNode[Mod[ListNode[U]]]]) => {
      tbd.read(lists, (lsts: ListNode[Mod[ListNode[T]]]) => {
        innerMap(tbd, dest, lsts, (list: Mod[ListNode[T]]) => {
          tbd.mod((dest: Dest[ListNode[U]]) => {
            tbd.read(list, (lst: ListNode[T]) => {
              innerMap(tbd, dest, lst, func)
            })
          })
        })
      })
    }))
  }

  private def innerMap[V, W](
      tbd: TBD,
      dest: Dest[ListNode[W]],
      lst: ListNode[V],
      f: V => W): Changeable[ListNode[W]] = {
    if (lst != null) {
      val newValue = tbd.mod((dest: Dest[W]) =>
        tbd.read(lst.value, (value: V) => tbd.write(dest, f(value))))
      val newNext = tbd.mod((dest: Dest[ListNode[W]]) =>
        tbd.read(lst.next, (next: ListNode[V]) => innerMap(tbd, dest, next, f)))
      tbd.write(dest, new ListNode[W](newValue, newNext))
    } else {
      tbd.write(dest, null)
    }
  }

  def parMap[U](tbd: TBD, func: T => U): Dataset[U] = {
    new Dataset(tbd.mod((dest: Dest[ListNode[Mod[ListNode[U]]]]) => {
      tbd.read(lists, (lsts: ListNode[Mod[ListNode[T]]]) => {
        innerParMap(tbd, dest, lsts, (tbd: TBD, list: Mod[ListNode[T]]) => {
          tbd.mod((dest: Dest[ListNode[U]]) => {
            tbd.read(list, (lst: ListNode[T]) => {
              innerParMap(tbd, dest, lst, (tbd: TBD, value: T) => func(value))
            })
          })
        })
      })
    }))
  }

  private def innerParMap[V, W](
      tbd: TBD,
      dest: Dest[ListNode[W]],
      lst: ListNode[V],
      f: (TBD, V) => W): Changeable[ListNode[W]] = {
    if (lst != null) {
	    val modTuple =
        tbd.par((tbd: TBD) => {
	        tbd.mod((valueDest: Dest[W]) => {
            tbd.read(lst.value, (value: V) => {
              tbd.write(valueDest, f(tbd, value))
            })
          })
	      }, (tbd: TBD) => {
          tbd.mod((nextDest: Dest[ListNode[W]]) => {
	          tbd.read(lst.next, (next: ListNode[V]) => {
              innerParMap(tbd, nextDest, next, f)
            })
          })
	      })
      tbd.write(dest, new ListNode[W](modTuple._1, modTuple._2))
    } else {
      tbd.write(dest, null)
    }
  }

  private def reduceHelper[U](
      tbd: TBD,
      dest: Dest[ListNode[U]],
      lst: ListNode[U],
      func: (U, U) => U): Changeable[ListNode[U]] = {
    if (lst == null) {
      tbd.write(dest, null)
    } else {
      tbd.read(lst.next, (next: ListNode[U]) => {
        if (next == null) {
          val newValue = tbd.mod((dest: Dest[U]) => {
            tbd.read(lst.value, (value: U) => tbd.write(dest, value))
          })
          val newNext = tbd.mod((dest: Dest[ListNode[U]]) => {
            tbd.write(dest, null)
          })
          tbd.write(dest, new ListNode(newValue, newNext))
        } else {
          val newValue = tbd.mod((dest: Dest[U]) => {
            tbd.read(lst.value, (value1: U) => {
              tbd.read(next.value, (value2: U) => {
                tbd.write(dest, func(value1, value2))
              })
            })
          })

          val newNext = tbd.mod((dest: Dest[ListNode[U]]) => {
            tbd.read(next.next, (lst: ListNode[U]) => {
              reduceHelper(tbd, dest, lst, func)
            })
          })
          tbd.write(dest, new ListNode(newValue, newNext))
        }
      })
    }
  }

  def reduce(tbd: TBD, func: (T, T) => T): Mod[T] = {
    def innerReduce[U](dest: Dest[U], lst: ListNode[U], f: (U, U) => U): Changeable[U] = {
      if (lst != null) {
        tbd.read(lst.next, (next: ListNode[U]) => {
          if (next != null) {
            val newList = tbd.mod((dest: Dest[ListNode[U]]) => {
              reduceHelper(tbd, dest, lst, f)
            })

            tbd.read(newList, (lst: ListNode[U]) => innerReduce(dest, lst, f))
          } else {
            tbd.read(lst.value, (value: U) => tbd.write(dest, value))
          }
        })
      } else {
        tbd.write(dest, null.asInstanceOf[U])
      }
    }

    val reducedLists: Mod[ListNode[Mod[T]]] = tbd.mod((dest: Dest[ListNode[Mod[T]]]) => {
      tbd.read(lists, (lsts: ListNode[Mod[ListNode[T]]]) => {
        innerMap(tbd, dest, lsts, (list: Mod[ListNode[T]]) => {
          tbd.mod((dest: Dest[T]) => {
            tbd.read(list, (lst: ListNode[T]) => {
              innerReduce(dest, lst, func)
            })
          })
        })
      })
    })

    val reducedMod = tbd.mod((dest: Dest[Mod[T]]) => {
      tbd.read(reducedLists, (reducedLsts: ListNode[Mod[T]]) => {
        innerReduce[Mod[T]](dest, reducedLsts, (mod1: Mod[T], mod2: Mod[T]) => {
          tbd.mod((dest: Dest[T]) => {
            tbd.read(mod1, (value1: T) => {
              tbd.read(mod2, (value2: T) => {
                tbd.write(dest, func(value1, value2))
              })
            })
          })
        })
      })
    })

    reducedMod.read()
  }

  private def parReduceHelper[U](
      tbd: TBD,
      dest: Dest[ListNode[U]],
      lst: ListNode[U],
      func: (TBD, U, U) => U,
      next: ListNode[U]): Changeable[ListNode[U]] = {
    if (next == null) {
      val newValue = tbd.mod((dest: Dest[U]) => {
        tbd.read(lst.value, (value: U) => tbd.write(dest, value))            
      })
      val newNext = tbd.mod((dest: Dest[ListNode[U]]) => tbd.write(dest, null))
      tbd.write(dest, new ListNode(newValue, newNext))
    } else {
      val modTuple = tbd.par((tbd:TBD) => {
        tbd.mod((dest: Dest[U]) => {
          tbd.read(lst.value, (value1: U) => {
            tbd.read(next.value, (value2: U) => {
              tbd.write(dest, func(tbd, value1, value2))
            })
          })
        })
      }, (tbd:TBD) => {
        tbd.mod((dest: Dest[ListNode[U]]) => {
          tbd.read(next.next, (lst: ListNode[U]) => {
	          if (lst == null) {
	            tbd.write(dest, null)
	          } else {
	            tbd.read(lst.next, (next: ListNode[U]) => {
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
    def innerReduce[U](
        tbd: TBD,
        dest: Dest[U],
        lst: ListNode[U],
        f: (TBD, U, U) => U): Changeable[U] = {
      if (lst != null) {
        tbd.read(lst.next, (next: ListNode[U]) => {
          if (next != null) {
            val newList = tbd.mod((dest: Dest[ListNode[U]]) => {
	            parReduceHelper(tbd, dest, lst, f, next)
	          })
            tbd.read(newList, (lst: ListNode[U]) => {
              innerReduce(tbd, dest, lst, f)
            })
          } else {
            tbd.read(lst.value, (value: U) => tbd.write(dest, value))
          }
        })
      } else {
        tbd.write(dest, null.asInstanceOf[U])
      }
    }

    val reducedLists = tbd.mod((dest: Dest[ListNode[Mod[T]]]) => {
      tbd.read(lists, (lsts: ListNode[Mod[ListNode[T]]]) => {
        innerParMap(tbd, dest, lsts, (tbd: TBD, list: Mod[ListNode[T]]) => {
          tbd.mod((dest: Dest[T]) => {
            tbd.read(list, (lst: ListNode[T]) => {
              innerReduce(tbd, dest, lst, (tbd: TBD, value1: T, value2: T) => {
                func(value1, value2)
              })
            })
          })
        })
      })
    })

    val reducedMod = tbd.mod((dest: Dest[Mod[T]]) => {
      tbd.read(reducedLists, (reducedLsts: ListNode[Mod[T]]) => {
        innerReduce(tbd, dest, reducedLsts, (tbd: TBD, mod1: Mod[T], mod2: Mod[T]) => {
          tbd.mod((dest: Dest[T]) => {
            tbd.read(mod1, (value1: T) => {
              tbd.read(mod2, (value2: T) => {
                tbd.write(dest, func(value1, value2))
              })
            })
          })
        })
      })
    })

    reducedMod.read()
  }
}
