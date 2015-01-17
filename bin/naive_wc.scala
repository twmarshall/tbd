#!/bin/sh
exec scala "$0" "$@"
!#

import java.io._
import java.util.regex.Pattern
import scala.collection.mutable.{Buffer, Map}

object Naive {
  def mapper(pair: (String, String)): (String, Int) = {
    var count = 0
    for (word <- pair._2.split("\\W+")) {
      count += 1
    }
    (pair._1, count)
  }

  def main(args: Array[String]) {
    val file = io.Source.fromFile("wiki.xml")

    val recordSeparator = 30.toChar
    val unitSeparator = 31.toChar
    val regex = Pattern.compile(
      recordSeparator + "(.*?)" + unitSeparator + "(.*?)" + recordSeparator)
    val matcher = regex.matcher(file.getLines.mkString)

    val counts = Map[String, Int]()
    while (matcher.find()) {
      for (word <- matcher.group(2).split("\\W+")) {
        if (counts.contains(word)) {
          counts(word) += 1
        } else {
          counts(word) = 1
        }
      }
    }

    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("naive-wc-output.txt"), "utf-8"))
    for ((key, value) <- counts.toBuffer.sortWith(_._1 < _._1)) {
      writer.write(key + " -> " + value + "\n")
    }
    writer.close()
  }
}
