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
package tdb.scripts

import org.rogach.scallop._
import scala.collection.mutable.Map
import scala.io.Source

import tdb.util.Util

object NaiveWC {
  def mapper
      (output: Map[String, Int],
       pair: (String, String)): Unit = {
    for (word <- pair._2.split("\\W+")) {
      if (output.contains(word)) {
        output(word) += 1
      } else {
        output(word) = 1
      }
    }
  }

 def main(args: Array[String]) {

    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val file = opt[String]("file", 'f', default = Some("enwiki.xml"),
        descr = "The file to read.")
      val output = opt[String]("output", 'o',
        default = Some("naive-wc-output"),
        descr = "The file to write the output to, .txt will be appended.")
      val updateFile = opt[String]("updateFile", default = Some("updates.xml"),
        descr = "The file to read updates from.")
      val updates = opt[List[Int]]("updates", 'u', default = Some(List()),
        descr = "The number of updates to perform for each round of change" +
        "propagation")
    }

    val file = Source.fromFile(Conf.file())

    val unitSeparator = 31.toChar

    val output = Map[String, Int]()
    for (line <- file.getLines) {
      val split = line.split(unitSeparator)
      mapper(output, (split(0), split(1)))
    }

    Util.writeMapToFile(Conf.output(), output)

    if (Conf.updates().size > 0) {
      val updateFile = Source.fromFile(Conf.updateFile())
      var lines = updateFile.getLines

      for (update <- Conf.updates()) {
        for (i <- 1 to update) {
          val split = lines.next.split(unitSeparator)
          mapper(output, (split(0), split(1)))
        }

        Util.writeMapToFile(Conf.output() + update, output)
      }
    }
  }
}
