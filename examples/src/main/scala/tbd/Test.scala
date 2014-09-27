package tbd.examples

import scala.collection.mutable.Buffer

object Test {
  val max = 100000

  def prependTest(): Long = {
    var buf = Buffer[Int]()
    val before = System.currentTimeMillis()

    for (i <- 1 to max) {
      buf +:= max
      val head = buf.head
    }

    val time = System.currentTimeMillis() - before
    println("prepend = " + time)
    time
  }

  def appendTest(): Long = {
    var buf = Buffer[Int]()
    val before = System.currentTimeMillis()

    for (i <- 1 to max) {
      buf :+= max
      val tail = buf.last
    }

    val time = System.currentTimeMillis() - before
    println("append = " + time)
    time
  }

  def main(args: Array[String]) {
    //(new StoreTest()).main()
    var pre = prependTest()
    var ap = appendTest()
    ap += appendTest()
    pre += prependTest()
    pre += prependTest()
    ap += appendTest()

    println("prepend: " + pre)
    println("append: " + ap)
  }
}
