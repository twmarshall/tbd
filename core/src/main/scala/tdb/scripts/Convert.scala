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

object Convert {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val file = opt[String]("file", 'f', default = Some("enwiki.xml"),
        descr = "The file to read.")
      val output = opt[String]("output", 'o', default = Some("enwiki2.xml"),
        descr = "The file to write the output to.")
      val partitions = opt[Int]("partitions", 'p', default = Some(4),
        descr = "The number of partitions to read the file in.")
    }

    val partitions = Conf.partitions()
    val output = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(Conf.output()), "utf-8"))

    for (i <- 0 until partitions) {
      val file = new File(Conf.file())
      val fileSize = file.length()

      val in = new BufferedReader(new FileReader(Conf.file()))
      val partitionSize = fileSize / partitions

      if (partitionSize > Int.MaxValue) {
        println("You need to set partitions to at least " + (fileSize / Int.MaxValue))
      }

      var buf = new Array[Char](partitionSize.toInt)

      in.skip(partitionSize * i)
      in.read(buf)

      val regex = Pattern.compile(
        "(?s)<title>(.*?)</title>.*?<text xml:space=\"preserve\">(.*?)</text>")
      val str = new String(buf)
      val matcher = regex.matcher(str)

      var end = 0

      val unitSeparator = 31.toChar
      val recordSeparator = 30.toChar
      while (matcher.find()) {
        val text = matcher.group(2).replace('\n', ' ')
        output.write(matcher.group(1) + unitSeparator + text + "\n")
        end = matcher.end()
      }

      if (i != partitions - 1) {
        var remaining = str.substring(end)
        var done = false
        while (!done) {
          in.read(buf)

          remaining += new String(buf)
          val regex2 = Pattern.compile(
            """(?s)<title>(.*?)</title>.*?<text(.*?)</text>""")
          val matcher2 = regex2.matcher(remaining)

          if (matcher2.find()) {
            val text = matcher2.group(2).replace('\n', ' ')
            output.write(matcher2.group(1) + unitSeparator + text + "\n")
            done = true
          }
        }
      }
    }

    output.close()
  }
}
