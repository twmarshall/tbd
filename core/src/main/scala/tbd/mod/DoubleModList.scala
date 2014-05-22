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

import scala.collection.mutable.{ArrayBuffer, Buffer}

import tbd.{Changeable, TBD}
import tbd.memo.Lift

class DoubleModList[T, V](
    aHead: Mod[DoubleModListNode[T, V]])
    extends AdjustableList[T, V]
    with Iterable[T, V, DoubleModListNode[T, V]] {
  val head = aHead

  def iterators(tbd: TBD) = {
    List(head)
  }

  def map[U, Q](
      tbd: TBD,
      f: (TBD, T, V) => (U, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[U, Q] = {
      
    if (parallel) {
      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.parMap(tbd, dest, f)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    } else {
      val lift = tbd.makeLift[Mod[DoubleModListNode[U, Q]]](!memoized)

      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.map(tbd, dest, f, lift)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }
  
  def split(
      tbd: TBD,
      pred: (TBD, T, V) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): 
       (AdjustableList[T, V], AdjustableList[T, V]) = {
    
    val result = tbd.mod2((matches: Dest[DoubleModListNode[T, V]], diffs: Dest[DoubleModListNode[T, V]]) => {
        
      tbd.read(head)(head => {
        if(head == null) {
          tbd.write(matches, null)
          tbd.write(diffs, null)
        } else {
          head.split(tbd, matches, diffs, pred)
        }
      })
    })
  
    (new DoubleModList(result._1), new DoubleModList(result._2))
  }

  def randomReduce(
      tbd: TBD,
      initialValueMod: Mod[(T, V)],
      f: (TBD, T, V, T, V) => (T, V),
      parallel: Boolean = false,
      memoized: Boolean = false): Mod[(T, V)] = {
    val zero = 0
    val halfLift = tbd.makeLift[Mod[DoubleModListNode[T, V]]](!memoized)

    tbd.mod((dest: Dest[(T, V)]) => {
      tbd.read(head)(head => {
        if(head == null) {
          tbd.read(initialValueMod)(initialValue =>
            tbd.write(dest, initialValue))
        } else {
          randomReduceList(tbd, head, zero, dest, halfLift, f)
        }
      })
    })
  }

  def randomReduceList(
      tbd: TBD,
      head: DoubleModListNode[T, V],
      round: Int,
      dest: Dest[(T, V)],
      lift: Lift[Mod[DoubleModListNode[T, V]]],
      f: (TBD, T, V, T, V) => (T, V)): Changeable[(T, V)] = {

    //I think round needs to be a mod, because we can have same values of 
    //value and next with different results accross rounds. 
    val halfListMod = lift.memo(List(head.next, head.value), () => {
      tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
        head.halfListReduce(tbd, round, new Hasher(2, 4), lift, dest, f)
      })
    })

    tbd.read(halfListMod)(halfList => {
      tbd.read(halfList.next)(next => {
        if(next == null) {
          tbd.read(halfList.value)(value =>
            tbd.write(dest, (halfList.key, value)))
        } else {
            randomReduceList(tbd, halfList,  round + 1, dest, lift, f)
        }
      })
    })
  }

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[(T, V)],
      f: (TBD, T, V, T, V) => (T, V),
      parallel: Boolean = false,
      memoized: Boolean = false): Mod[(T, V)] = {
    randomReduce(tbd, initialValueMod, f, parallel, memoized)
  }

  def filter(
      tbd: TBD,
      pred: (T, V) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): DoubleModList[T, V] = {
    val lift = tbd.makeLift[Mod[DoubleModListNode[T, V]]](!memoized)

    new DoubleModList(
      tbd.mod((dest: Dest[DoubleModListNode[T, V]]) => {
        tbd.read(head)(node => {
          if (node != null) {
            node.filter(tbd, dest, pred, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      })
    )
  }

  def toBuffer(): Buffer[V] = {
    val buf = ArrayBuffer[V]()
    var node = head.read()
    while (node != null) {
      buf += node.value.read()
      node = node.next.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
