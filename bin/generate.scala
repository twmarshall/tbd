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
    val s = rs + "1" + us + "asdf" +
            rs + "2" + us + "fdsa" +
            rs + "3" + us + "qwer" +
            rs + "4" + us + "rewq" +
            rs + "5" + us + "zxcv" +
            rs + "6" + us + "vcxz" +
            rs + "7" + us + "uiop" +
            rs + "8" + us + "poiu" + rs
    output.write(s)

    output.close()
  }
}
