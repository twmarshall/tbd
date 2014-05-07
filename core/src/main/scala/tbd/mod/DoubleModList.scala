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

class DoubleModList[T](
    aHead: Mod[DoubleModListNode[T]]) extends AdjustableList[T] {
  val head = aHead

  def map[U](
      tbd: TBD,
      f: (TBD, T) => U,
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[U] = {
    if (parallel) {
      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[U]]) => {
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
      if (memoized) {
        val lift = tbd.makeLift[Mod[DoubleModListNode[U]]]()

        new DoubleModList(
          tbd.mod((dest: Dest[DoubleModListNode[U]]) => {
            tbd.read(head)(node => {
              if (node != null) {
                node.memoMap(tbd, dest, f, lift)
              } else {
                tbd.write(dest, null)
              }
            })
          })
        )
      } else {
        new DoubleModList(
          tbd.mod((dest: Dest[DoubleModListNode[U]]) => {
            tbd.read(head)(node => {
              if (node != null) {
                node.map(tbd, dest, f)
              } else {
                tbd.write(dest, null)
              }
            })
          })
        )
      }
    }
  }

  def foldl(
      tbd: TBD,
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Mod[T] = {
    tbd.mod((dest: Dest[T]) => {
      tbd.read(head)(node => {
        if(node != null) {
          node.foldl(tbd, dest, initialValueMod, f)
        } else {
          tbd.read(initialValueMod)(initialValue =>
            tbd.write(dest, initialValue))
        }
      })
    })
  }

  type HalfList = (T, Mod[IdListNode])
  type IdListNode = DoubleModListNode[(ModId, T)]

  def randomReduce(
      tbd: TBD,
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Mod[T] = {
    val idList = toIdList(tbd, head)
    val zero = tbd.createMod(0)
    val halfLift = tbd.makeLift[Mod[HalfList]]()
    val reduceLift = tbd.makeLift[Mod[T]]()
    
    tbd.mod((dest: Dest[T]) => {
      randomReduceIdList(tbd, initialValueMod, 
                         idList, zero, dest, 
                         halfLift, reduceLift, f)
   }) 
  }

  def toIdList(
      tbd: TBD,  
      head: Mod[DoubleModListNode[T]]): Mod[IdListNode] = {
    tbd.mod((dest: Dest[IdListNode]) => {
      tbd.read(head)(head => {
        if(head == null) {
          tbd.write(dest, null)
        } else {
          tbd.read(head.valueMod)(value => {
            val newValue = tbd.createMod((head.valueMod.id, value))
            val newList = toIdList(tbd, head.next)
            tbd.write(dest, new IdListNode(newValue, newList))
          })
        }
      })
    })
  }

  def randomReduceIdList(
      tbd: TBD,
      identityMod: Mod[T],
      headMod: Mod[IdListNode],
      round: Mod[Int],
      dest: Dest[T],
      halfLift: Lift[Mod[HalfList]],
      reduceLift: Lift[Mod[T]],
      f: (TBD, T, T) => T): Changeable[T] = {
    
    tbd.read(headMod)(head => {
      if(head != null) {
     
        var fixedVars = List(headMod, round, identityMod) 
        val halfList = halfLift.memo(fixedVars, () => {
          tbd.mod((dest: Dest[HalfList]) =>
            halfIdList(tbd, identityMod, head, round, dest, f))
        })

        tbd.read(halfList)(halfList => {
          
          fixedVars = List(headMod, halfList._2, round, identityMod)
          val result = reduceLift.memo(fixedVars, () => {
            tbd.mod((dest: Dest[T]) => 
              randomReduceIdList(tbd, identityMod, halfList._2,
                                 tbd.increment(round), dest,
                                 halfLift, reduceLift, f))  
          })
          
          tbd.read(result)(result =>     
            tbd.write(dest, f(tbd, halfList._1, result)))
        })

      } else {
        tbd.read(identityMod)(identity => {
          tbd.write(dest, identity) 
        })
      }
    })
  }

  def halfIdList(
      tbd: TBD,
      identityMod: Mod[T],
      head: IdListNode,
      round: Mod[Int],
      dest: Dest[HalfList],
      f: (TBD, T, T) => T): Changeable[HalfList] = {
    if(head != null) {
      tbd.read2(head.valueMod, head.next)((value, next) => {
        val halfResult = tbd.mod((dest: Dest[HalfList]) =>
          halfIdList(tbd, identityMod, next, round, dest, f))
        tbd.read2(round, halfResult)((round, halfResult) => {
          if(binaryHash(value._1, round)) {
            val newList = tbd.createMod(
              new DoubleModListNode(
                tbd.createMod((value._1, value._2)), 
                halfResult._2))
            tbd.write(dest, (halfResult._1, newList))
          } else {
            tbd.write(dest, (f(tbd, halfResult._1, value._2), halfResult._2))
          }
        })
      })
    } else { 
      tbd.read(identityMod)(identity => {
        tbd.write(dest, (identity, tbd.createMod(head))) 
      })
    }
  }

  def binaryHash(id: ModId, round: Int) = {
    var k:Int = round
    (k > 32) || ((id.value.hashCode() >> k) % 2 == 0)
  }

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[T],
      f: (TBD, T, T) => T): Mod[T] = {
    randomReduce(tbd, initialValueMod, f)
  }

  def filter(
      tbd: TBD,
      pred: T => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[T] = {
    if (memoized) {
      val lift = tbd.makeLift[Mod[DoubleModListNode[T]]]()

      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[T]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.memoFilter(tbd, dest, pred, lift)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    } else {
      new DoubleModList(
        tbd.mod((dest: Dest[DoubleModListNode[T]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.filter(tbd, dest, pred)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }

  def toBuffer(): Buffer[T] = {
    val buf = ArrayBuffer[T]()
    var node = head.read()
    while (node != null) {
      buf += node.valueMod.read()
      node = node.next.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
