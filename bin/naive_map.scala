#!/bin/sh
exec scala "$0" "$@"
!#

import java.io._
import java.util.regex.Pattern
import scala.collection.mutable.Buffer

object Naive {
  def mapper(pair: (String, String)): (String, Int) = {
    var count = 0
    for (word <- pair._2.split("\\W+")) {
      count += 1
    }
    (pair._1, count)
  }

  def main(args: Array[String]) {
    val file = io.Source.fromFile("enwiki1.xml")

    val recordSeparator = 30.toChar
    val unitSeparator = 31.toChar
    val regex = Pattern.compile(
      recordSeparator + "(.*?)" + unitSeparator + "(.*?)" + recordSeparator)
    val matcher = regex.matcher(file.getLines.mkString)

    val output = Buffer[(String, Int)]()
    while (matcher.find()) {
      output += mapper((matcher.group(1), matcher.group(2)))
    }

    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("naive-output.txt"), "utf-8"))
    for ((key, value) <- output.sortWith(_._1 < _._1)) {
      writer.write(key + " -> " + value + "\n")
    }
    writer.close()
  }
}
