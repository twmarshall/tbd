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
package tbd.examples.list

import scala.collection.mutable.Map

import tbd.{Adjustable, TBD}
import tbd.mod.ModList

object MapAdjust {
  def mapper(s: String): Int = {
    var count = 0
    for (word <- s.split("\\W+")) {
      count += 1
    }
    return count
  }
}

class MapAdjust(partitions: Int) extends WC with Adjustable {
  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.map(tbd, (s: String) => MapAdjust.mapper(s))
  }
}

class MapMemoAdjust(partitions: Int) extends WC with Adjustable {
  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.memoMap(tbd, (s: String) => MapAdjust.mapper(s))
  }
}

class MapParAdjust(partitions: Int) extends WC with Adjustable {
  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.parMap(tbd, (s: String) => MapAdjust.mapper(s))
  }
}

class MapMemoParAdjust(partitions: Int) extends WC with Adjustable {
  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.memoParMap(tbd, (s: String) => MapAdjust.mapper(s))
  }
}
