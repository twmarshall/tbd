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

import akka.actor.ActorRef

import tbd.{Changeable, Dest, TBD}

class Matrix(aMat: Array[Array[Mod[Int]]], modStoreRef: ActorRef) {
  def this(aMat: Array[Array[Int]], modStoreRef: ActorRef) =
    this(aMat.map(row => row.map(cell => new Mod(cell, modStoreRef))),
         modStoreRef)

  val mat = aMat

  def mult(tbd: TBD, that: Matrix): Matrix = {
    val arr = new Array[Array[Mod[Int]]](mat.size)

    for (i <- 0 to mat.size - 1) {
      arr(i) = new Array[Mod[Int]](that.mat(0).size)
      for (j <- 0 to that.mat(0).size - 1) {
        def recur(k: Int, dest: Dest): Changeable[Int] = {
          if (k >= mat(0).size)
            tbd.write(dest, 0)
          else
            tbd.read(mat(i)(k),
                     (value1: Int) => {
                       val next = tbd.mod(dest => recur(k+1, dest))
                       tbd.read(that.mat(k)(j),
                                (value2: Int) => {
                                  tbd.read(next,
                                           (nextValue: Int) => {
                                             tbd.write(dest, value1 * value2 + nextValue)
                                           })
                                })
                     })
        }
        arr(i)(j) = tbd.mod(dest => recur(0, dest))
      }
    }
    new Matrix(arr, modStoreRef)
  }

  override def toString: String = {
    mat.map(row => row.map(cell => cell.read().toString)
      .reduceLeft(_ + " " + _)).reduceLeft(_ + "\n" + _)
  }
}
