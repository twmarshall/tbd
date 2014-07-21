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

package tbd.visualization

import tbd.mod.{Mod, Dest}
import tbd.{Memoizer, TBD}
import scala.collection.mutable.Map


class ListNode(_value: Mod[Int], _next: Mod[ListNode]) {
    val value = _value
    val next = _next

    override def toString() = {
      "ListNode"
    }
}

class MinimapAlgorithm extends TestAlgorithm[Mod[ListNode], Seq[Int]] {

  def run(tbd: TBD): Mod[ListNode] = {
    val head = tbd.mod((dest: Dest[ListNode]) => {
      tbd.write(dest, new ListNode(tbd.createMod(1), tbd.mod((dest: Dest[ListNode]) => {
        tbd.write(dest, new ListNode(tbd.createMod(2), tbd.mod((dest: Dest[ListNode]) => {
          tbd.write(dest, new ListNode(tbd.createMod(3), tbd.mod((dest: Dest[ListNode]) => {
                tbd.write(dest, null)
          })))
        })))
      })))
    })

    incrementList(tbd, head)
  }

  def incrementList(tbd: TBD, head: Mod[ListNode]): Mod[ListNode] = {
      val lift = tbd.makeMemoizer[Mod[ListNode]]()
      incrementRecursive(tbd, head, lift)
  }

  def incrementRecursive(tbd: TBD, current: Mod[ListNode], lift: Memoizer[Mod[ListNode]])
      : Mod[ListNode] = {

    tbd.mod((dest: Dest[ListNode]) => {
        tbd.read(current)(current => {
            if(current == null) {
                tbd.write(dest, null)
            } else {
                val newValue = tbd.mod((destValue: Dest[Int]) => {
                    tbd.read(current.value)(value => {
                      tbd.write(destValue, value + 1)
                    })
                })

                val newNext = lift(current.next) {
                    incrementRecursive(tbd, current.next, lift)
                }

                tbd.write(dest, new ListNode(newValue, newNext))
            }
        })
    })
  }

  def getResult(output: Mod[ListNode]): Seq[Int] = {
    Seq(2, 4, 6)
  }

  def getExpectedResult(input: Map[Int, Int]): Seq[Int] = {
    Seq(2, 4, 6)
  }
}
