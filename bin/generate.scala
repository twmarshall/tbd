#!/bin/sh
exec scala "$0" "$@"
!#

import java.io._

object Generate {
  def main(args: Array[String]) {
    val output = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("wiki.xml"), "utf-8"))

    val us = 31.toChar
    val s = "1" + us + "asdf\n" +
            "2" + us + "fdsa\n" +
            "3" + us + "qwer\n" +
            "4" + us + "rewq\n" +
            "5" + us + "zxcv\n" +
            "6" + us + "vcxz\n" +
            "7" + us + "uiop\n" +
            "8" + us + "poiu"
    output.write(s)

    output.close()
  }
}
