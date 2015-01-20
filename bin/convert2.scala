#!/bin/sh
exec scala "$0" "$@"
!#

import java.io._
import java.util.regex.Pattern

object Convert2 {
  def main(args: Array[String]) {
    val partitions = 4
    for (num <- List("08")) {
    val fileName = "wiki/enwiki-201410" + num + "-pages-meta-hist-incr.xml"

    val output = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("w/enwiki-updates-" + num + ".xml"), "utf-8"))

    for (i <- 0 until partitions) {
      val file = new File(fileName)
      val fileSize = file.length()

      val in = new BufferedReader(new FileReader(fileName))
      val partitionSize = fileSize / partitions
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
        output.write(recordSeparator + matcher.group(1) +
            unitSeparator + text + recordSeparator + "\n")
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
            output.write(recordSeparator + matcher2.group(1) + unitSeparator +
                text + recordSeparator + "\n")
            done = true
          }
        }
      }
    }

    output.close()
}
  }
}
