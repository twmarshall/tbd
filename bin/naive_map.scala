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
    val file = io.Source.fromFile("enwiki9.xml")

    val unitSeparator = 31.toChar

    val output = Buffer[(String, Int)]()
    for (line <- file.getLines) {
      val split = line.split(unitSeparator)
      output += mapper((split(0), split(1)))
    }

    val writer = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("naive-output.txt"), "utf-8"))
    for ((key, value) <- output.sortWith(_._1 < _._1)) {
      writer.write(key + " -> " + value + "\n")
    }
    writer.close()
  }
}
