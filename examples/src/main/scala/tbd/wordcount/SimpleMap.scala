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

import akka.actor._
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

class ListNode[T](aValue: T, aNext: ListNode[T]) {
  val value = aValue
  val next = aNext
}

object Worker {
  def props(): Props =
    Props(classOf[Worker])
}

case class RunMessage(tail: ListNode[String])

class Worker extends Actor {
  val wc = new WC()

  def map[T, U](lst: ListNode[T], f: T => U): ListNode[U] = {
    if (lst.next != null)
      new ListNode[U](f(lst.value), map(lst.next, f))
    else
      new ListNode[U](f(lst.value), null)
  }

  def receive = {
    case RunMessage(tail: ListNode[String]) =>
      sender ! map(tail, wc.wordcount((_: String)))
  }
}

object SimpleMap {
  def main(args: Array[String]) {
    //val system = ActorSystem("masterSystem")
    //val workerRef = system.actorOf(Worker.props(), "master")

    val xml = scala.xml.XML.loadFile("wiki.xml")
    var i = 0
    val count = args(0).toInt

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

    val before = System.currentTimeMillis()
    /*val mapped = scala.collection.mutable.Set[scala.collection.mutable.Map[String, Int]]()
    for (page <- pages) {
      mapped += wc.wordcount(page)
    }*/
    val wc = new WC()
    def map[T, U](lst: ListNode[T], f: T => U): ListNode[U] = {
      if (lst.next != null)
        new ListNode[U](f(lst.value), map(lst.next, f))
      else
        new ListNode[U](f(lst.value), null)
    }
    val mapped = map(tail, wc.wordcount((_: String)))

    /*implicit val timeout = Timeout(30 seconds)

    val future = workerRef ? RunMessage(tail)
    Await.result(future, timeout.duration)*/

    println(System.currentTimeMillis() - before)
  }
}
