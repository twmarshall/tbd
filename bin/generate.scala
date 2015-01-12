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
    val s = rs + "1" + us + "asdf" + rs +
            rs + "2" + us + "fdsa" + rs +
            rs + "3" + us + "qwer" + rs +
            rs + "4" + us + "rewq" + rs +
            rs + "5" + us + "zxcv" + rs +
            rs + "6" + us + "vcxz" + rs +
            rs + "7" + us + "uiop" + rs +
            rs + "8" + us + "poiu" + rs
    output.write(s)

    output.close()
  }
}
