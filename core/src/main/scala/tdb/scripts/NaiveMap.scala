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

import java.io._
import java.util.regex.Pattern
import org.rogach.scallop._
import scala.collection.mutable.Buffer

object NaiveMap {
  def mapper(pair: (String, String)): (String, Int) = {
    var count = 0
    for (word <- pair._2.split("\\W+")) {
      count += 1
    }
    (pair._1, count)
  }

  def main(args: Array[String]) {

    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val file = opt[String]("file", 'f', default = Some("enwiki.xml"),
        descr = "The file to read.")
      val output = opt[String]("output", 'o', default = Some("naive-map-output.xml"),
        descr = "The file to write the output to.")
    }

    val file = scala.io.Source.fromFile(Conf.file())

    val unitSeparator = 31.toChar

    val output = Buffer[(String, Int)]()
    for (line <- file.getLines) {
      val split = line.split(unitSeparator)
      output += mapper((split(0), split(1)))
    }

    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(Conf.output()), "utf-8"))
    for ((key, value) <- output.sortWith(_._1 < _._1)) {
      writer.write(key + " -> " + value + "\n")
    }
    writer.close()
  }
}
