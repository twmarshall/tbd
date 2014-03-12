/**
 * Copyright (C) 2013 Carnegie Mellon University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tbd.examples.wordcount

class ListNode[T](aValue: T, aNext: ListNode[T]) {
  val value = aValue
  val next = aNext
}

object SimpleMap {
  def run(count: Int, repeat: Int) {
    val xml = scala.xml.XML.loadFile("wiki.xml")
    var i = 0

    val pages = scala.collection.mutable.Set[String]()
    var tail: ListNode[String] = null
    (xml \ "elem").map(elem => {
      (elem \ "key").map(key => {
        (elem \ "value").map(value => {
          if (i < count) {
            tail = new ListNode(value.text, tail)
            pages += value.text
            i += 1
          }
        })
      })
    })

    def map[T, U](lst: ListNode[T], f: T => U): ListNode[U] = {
      if (lst.next != null)
        new ListNode[U](f(lst.value), map(lst.next, f))
      else
        new ListNode[U](f(lst.value), null)
    }

    // Warmup run.
    val wc = new WC()
    val mapped = map(tail, wc.wordcount((_: String)))

    var j = 0
    var total: Long = 0
    while (j < repeat) {
      val before = System.currentTimeMillis()

      val wc = new WC()
      val mapped = map(tail, wc.wordcount((_: String)))

      total += System.currentTimeMillis() - before

      j += 1
    }

    println("smap\t" + count + "\t" + total / repeat)
  }
}
