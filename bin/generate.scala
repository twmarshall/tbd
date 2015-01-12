#!/bin/sh
exec scala "$0" "$@"
!#

import java.io._

object Generate {
  def main(args: Array[String]) {
    val output = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("wiki.xml"), "utf-8"))

    val rs = 30.toChar
    val us = 31.toChar
    val s = rs + "1" + us + "asdf\n" +
            rs + "2" + us + "fdsa\n" +
            rs + "3" + us + "qwer\n" +
            rs + "4" + us + "rewq\n" +
            rs + "5" + us + "zxcv\n" +
            rs + "6" + us + "vcxz\n" +
            rs + "7" + us + "uiop\n" +
            rs + "8" + us + "poiu" + rs
    output.write(s)

    output.close()
  }
}
