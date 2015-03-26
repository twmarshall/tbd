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
import org.rogach.scallop._

import tdb.Constants._
import tdb.util._

object SplitUpdates {
  def main(args: Array[String]) {

    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val dir = opt[String]("dir", 'd', default = Some(""),
        descr = "The directory to write the output to.")
      val file = opt[String]("file", 'f', default = Some("enwiki.xml"),
        descr = "The file to read.")
      val repeat = opt[Int]("repeat", 'r', default = Some(5),
        descr = "The number of times to repeat each update.")
      val updates = opt[List[Int]]("updates", 'u',
        default = Some(List(10, 20, 30, 40)),
        descr = "The size of the files to create.")
    }

    if (!OS.exists(Conf.dir())) {
      OS.mkdir(Conf.dir())
    }

    var count = 0
    var repeat = 0
    var total = Conf.updates().head
    var remainingUpdates = Conf.updates().tail
    var writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(Conf.dir() + "/" + total + "-" + repeat), "utf-8"))
    var done = false
    def process(key: String, value: String) {
      if (!done) {
        if (count < total) {
          writer.write(key + unitSeparator + value + "\n")
          count += 1
        } else {
          writer.close()

          if (repeat == Conf.repeat() - 1) {
            if (remainingUpdates.size == 0) {
              done = true
            } else {
              count = 0
              repeat = 0
              total = remainingUpdates.head
              remainingUpdates = remainingUpdates.tail
              writer = new BufferedWriter(
                new OutputStreamWriter(
                  new FileOutputStream(
                    Conf.dir() + "/" + total + "-" + repeat), "utf-8"))
            }
          } else {
            count = 0
            repeat += 1
            writer = new BufferedWriter(
              new OutputStreamWriter(
                new FileOutputStream(
                  Conf.dir() + "/" + total + "-" + repeat), "utf-8"))
          }
        }
      }
    }

    FileUtil.readEntireKeyValueFile(Conf.file(), process)
  }
}
