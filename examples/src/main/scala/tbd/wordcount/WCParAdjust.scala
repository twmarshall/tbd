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
package tbd.examples.wordcount

import scala.collection.mutable.Map

import tbd.{Adjustable, TBD}
import tbd.mod.Mod

class WCParAdjust(partitions: Int) extends WC with Adjustable {
  def run(tbd: TBD): Mod[Map[String, Int]] = {
    val pages = tbd.input.getDataset[String](partitions)
    val counts = pages.parMap(tbd, (s: String) => wordcount(s))
    counts.parReduce(tbd, reduce((_: Map[String, Int]), (_: Map[String, Int])))
  }
}
