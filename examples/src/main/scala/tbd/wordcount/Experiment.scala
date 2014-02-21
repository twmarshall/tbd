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

import tbd.{Adjustable, Mutator}
import tbd.mod.Mod

object Experiment {
  def run(adjust: Adjustable, pages: Int, description: String) {
    val counts = Array(10, 50, 100)
    val percents = Array(0.1, 0.2, 0.3)
    val r = new scala.util.Random()

    for (count <- counts) {
      val mutator = new Mutator()

      val xml = scala.xml.XML.loadFile("wiki.xml")
      var i = 0

      val pages = scala.collection.mutable.Map[String, String]()
      (xml \ "elem").map(elem => {
        (elem \ "key").map(key => {
          (elem \ "value").map(value => {
            if (i < count) {
              mutator.put(key.text, value.text)
              i += 1
            } else {
              pages += (key.text -> value.text)
            }
          })
        })
      })

      val before = System.currentTimeMillis()
      val output = mutator.run[Mod[Map[String, Int]]](adjust)
      println("Initial run for " + description + " on " + count + " pages = " +
              (System.currentTimeMillis() - before) + "ms")

      for (percent <- percents) {
        var i =  0
        while (i < percent * count) {
          mutator.update(r.nextInt(count).toString, pages.head._2)
          pages -= pages.head._1
          i += 1
        }
        val before2 = System.currentTimeMillis()
        mutator.propagate()
        println("Propagatation for updating " + percent + "% pages = " +
                (System.currentTimeMillis() - before2) + "ms")          
      }

      mutator.shutdown()
    }
  }

  def main(args: Array[String]) {
    val pages =
      if (args.size == 1) {
        args(0).toInt
      } else {
        10
      }

    run(new WCAdjust(), pages, "nonparallel")

    run(new WCParAdjust(), pages, "parallel")
  }
}
