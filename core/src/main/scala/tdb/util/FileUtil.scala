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
package tdb.util

import java.io._
import java.util.regex.Pattern

import tdb.Constants._

object FileUtil {
  /**
   * Read file fileName starting at position readFrom, with length readSize. If
   * this ends in the middle of a pair, it will keep reading until it finds the
   * end of the current pair.
   *
   * The file must be in a key value format with Constants.recordSeparator
   * before and after each pair and Constants.unitSeparator between each key
   * and value.
   *
   * The process function is called on each key value pair.
   */
  def readKeyValueFile
      (fileName: String,
       fileSize: Long,
       readFrom: Long,
       readSize: Long,
       process: (String, String) => Unit) {
    val in = new BufferedReader(new FileReader(fileName))
    in.skip(readFrom)

    var read = 0L
    if (readFrom != 0) {
      read += in.readLine().length()
    }

    var line = in.readLine()
    while (read < readSize && line != null) {
      read += line.length()

      val split = line.split(unitSeparator)
      process(split(0), split(1))

      line = in.readLine()
    }
  }

  def readEntireKeyValueFile
      (fileName: String, process: (String, String) => Unit) {
    val f = new File(fileName)
    readKeyValueFile(fileName, f.length, 0, f.length, process)
  }

  def getBytes(path: String): Array[Byte] = {
    val f = new File(path)

    if (!f.exists) {
      Array[Byte]()
    } else {
      val buf = new BufferedInputStream(new FileInputStream(path))
      val arr = new Array[Byte](f.length().toInt)
      buf.read(arr)

      arr
    }
  }
}
