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

import scala.collection.mutable.ArrayBuffer
import tbd.{Adjustable, Changeable, Mutator, TBD}
import tbd.mod.{AdjustableList, Dest, Mod}
import collection.mutable.HashMap
import scala.util.Random

class AsyncModTest {
  val mutator = new Mutator()


  def run() {

    val mutator = new Mutator()
//    mutator.put(1, 1)
//
//    val output = mutator.run[Mod[Int]](new AsyncTest())
//    println("Result 1: " + output.read())
//
//
//    mutator.update(1, 2)
//    mutator.propagate()
//
//    println("Result 2: " + output.read())

    mutator.shutdown()
  }
}
