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
        lsts.map(tbd, dest, (list: Mod[ListNode[T]]) => {
          tbd.mod((dest: Dest[ListNode[U]]) => {
            tbd.read(list, (lst: ListNode[T]) => {
              lst.map(tbd, dest, func)
            })
          })
        })
      })
    }))
  }

  def parMap[U](tbd: TBD, func: T => U): Dataset[U] = {
    new Dataset(tbd.mod((dest: Dest[ListNode[Mod[ListNode[U]]]]) => {
      tbd.read(lists, (lsts: ListNode[Mod[ListNode[T]]]) => {
        lsts.parMap(tbd, dest, (tbd: TBD, list: Mod[ListNode[T]]) => {
          tbd.mod((dest: Dest[ListNode[U]]) => {
            tbd.read(list, (lst: ListNode[T]) => {
              lst.map(tbd, dest, (value: T) => func(value))
            })
          })
        })
      })
    }))
  }

  def reduce(tbd: TBD, func: (T, T) => T): Mod[T] = {
    def innerReduce[U](dest: Dest[U], lst: ListNode[U], f: (U, U) => U): Changeable[U] = {
      if (lst != null) {
        tbd.read(lst.next, (next: ListNode[U]) => {
          if (next != null) {
            val newList = tbd.mod((dest: Dest[ListNode[U]]) => {
              lst.reduce(tbd, dest, f)
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
        lsts.map(tbd, dest, (list: Mod[ListNode[T]]) => {
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

    tbd.mod((dest: Dest[T]) => {
      tbd.read(reducedMod, (mod: Mod[T]) => {
        tbd.read(mod, (value: T) => {
          tbd.write(dest, value)
        })
      })
    })
  }

  def parReduce(tbd: TBD, func: (T, T) => T): Mod[T] = {
    def innerReduce[U](
        tbd: TBD,
        dest: Dest[U],
        lst: ListNode[U],
        f: (U, U) => U): Changeable[U] = {
      if (lst != null) {
        tbd.read(lst.next, (next: ListNode[U]) => {
          if (next != null) {
            val newList = tbd.mod((dest: Dest[ListNode[U]]) => {
	            lst.reduce(tbd, dest, f)
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
        // Do a parallel map over the partitions, where the mapping function is
        // reduce.
        lsts.parMap(tbd, dest, (tbd: TBD, list: Mod[ListNode[T]]) => {
          tbd.mod((dest: Dest[T]) => {
            tbd.read(list, (lst: ListNode[T]) => {
              innerReduce(tbd, dest, lst, (value1: T, value2: T) => {
                func(value1, value2)
              })
            })
          })
        })
      })
    })

    val reducedMod = tbd.mod((dest: Dest[Mod[T]]) => {
      tbd.read(reducedLists, (reducedLsts: ListNode[Mod[T]]) => {
        innerReduce(tbd, dest, reducedLsts, (mod1: Mod[T], mod2: Mod[T]) => {
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

    tbd.mod((dest: Dest[T]) => {
      tbd.read(reducedMod, (mod: Mod[T]) => {
        tbd.read(mod, (value: T) => {
          tbd.write(dest, value)
        })
      })
    })
  }
}
