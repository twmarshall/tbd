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
import scala.collection.mutable.Buffer

import tdb.Constants.unitSeparator
import tdb.util.FileUtil

object Split {
  def main(args: Array[String]) {
    object Conf extends ScallopConf(args) {
      version("TDB 0.1 (c) 2014 Carnegie Mellon University")
      val file = opt[String]("file", 'f', default = Some("enwiki.xml"),
        descr = "The file to read.")
      val partitions = opt[Int]("partitions", 'p', default = Some(2),
        descr = "The number of partitions to split the file into.")
    }

    val outputs = Buffer[BufferedWriter]()
    for (i <- 0 until Conf.partitions()) {
      outputs += new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream(Conf.file() + "-" + i), "utf-8"))
    }

    val file = new File(Conf.file())
    val fileSize = file.length()

    def process = (key: String, value: String) => {
      outputs(key.hashCode().abs % Conf.partitions())
        .write(key + unitSeparator + value + "\n")
    }

    FileUtil.readKeyValueFile(Conf.file(), fileSize, 0, fileSize, process)

    for (i <- 0 until Conf.partitions()) {
      outputs(i).close()
    }
  }
}
