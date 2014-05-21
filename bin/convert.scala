#!/bin/sh
exec scala "$0" "$@"
!#

import java.io._
import scala.xml.XML

object Convert {
  def main(args: Array[String]) {
    val elems = XML.loadFile("wiki.xml")
    val output = new BufferedWriter(new OutputStreamWriter(
      new FileOutputStream("wiki2.xml"), "utf-8"))

    output.write("<elems>")
    (elems \\ "elem").map(elem => {
      (elem \\ "key").map(key => {
        (elem \\ "value").map(value => {
          if (value.text.size > 500) {
            output.write("<elem><key>" + key.text + "</key><value>" +
                         value + "</value></elem>")
          } else {
            println(value.text)
          }
        })
      })
    })
    output.write("</elems>")
    output.close()
  }
}
