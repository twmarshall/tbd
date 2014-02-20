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

import tbd.Mutator
import tbd.mod.Mod

object Experiment {
  def main(args: Array[String]) {
    val mutator = new Mutator()

    val pages =
      if (args.size == 1) {
        args(0).toInt
      } else {
        10
      }

    mutator.load("wiki.xml", pages)
    val before = System.currentTimeMillis()
    val output = mutator.run[Mod[Map[String, Int]]](new WCAdjust())
    println("\n\nInitial run for nonparallel on " + pages + " pages = " +
            (System.currentTimeMillis() - before) + "ms\n\n")

    mutator.put("qwer", "qwer qwer")
    val before2 = System.currentTimeMillis()
    mutator.propagate()
    println("\n\nPropagatation for inserting " + (1.0 / pages * 100) + "% pages = " +
            (System.currentTimeMillis() - before2) + "ms\n\n")

    mutator.shutdown()
  }
}
