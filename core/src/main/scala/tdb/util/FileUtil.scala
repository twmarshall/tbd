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

import java.io.{BufferedReader, FileReader}
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

    var read = 0
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

    /*val numReads =
      if (readSize > Int.MaxValue) {
        (readSize / Int.MaxValue).toInt + 1
      } else {
        1
      }

    in.skip(readFrom)
    print("1>")
    scala.io.StdIn.readLine()
    val regex = Pattern.compile(
      recordSeparator + "(.*?)" + unitSeparator + "(.*?)" + recordSeparator)

    var end = 0
    var str = ""
    for (i <- 1 to numReads) {
      val thisReadSize =
        if (i == numReads && readSize % numReads != 0) {
          readSize % numReads
        } else {
          readSize / numReads
        }
      print("2> " + thisReadSize)
      scala.io.StdIn.readLine()

      // used: 377,000,000
      val buf = new Array[Char](thisReadSize.toInt)


      print("2.5>")
      scala.io.StdIn.readLine()

      in.read(buf)



      print("3>")
      scala.io.StdIn.readLine()

      str = new String(buf)

      print("4>")
      scala.io.StdIn.readLine()

      val matcher = regex.matcher(str)

      print("5>")
      scala.io.StdIn.readLine()

      var nextList = 0
      while (matcher.find()) {
        process(matcher.group(1), matcher.group(2))
        end = matcher.end()
      }
    }

    if (readFrom + readSize < fileSize) {
      var remaining = str.substring(end)
      var done = false
      while (!done) {
        val smallBuf = new Array[Char](4096)
        in.read(smallBuf)

        remaining += new String(smallBuf)
        val matcher = regex.matcher(remaining)
        if (matcher.find()) {
          process(matcher.group(1), matcher.group(2))
          done = true
        }
      }
    }*/
  }
}
