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
import collection.mutable.HashMap
import scala.util.Random
import scala.io.StdIn

import tbd._

class TargetedExhaustiveTest[T, V](algorithm: TestAlgorithm[T, V]) extends TestBase(algorithm) {

  def initialize() = { }

  private var len = 1
  private var pos = 1
  var repetitions = 5
  private var ec = 0

  def step() = {
    if(ec == repetitions) {
      pos += 1
      if(pos + len > initialSize + 1)
      {
        pos = 1
        len += 1
      }
      ec = 1
    } else {
      ec += 1
    }

    if(len <= initialSize) {
      for(i <- pos to (pos + len - 1)) {
        updateValue(i, rand.nextInt(maxValue))
      }
      true
    } else {
      false
    }
  }

  def dispose() = { }
}
