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
package tbd

import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._

import tbd.master.Main
import tbd.messages._
import tbd.mod.Mod

class Mutator {
  val main = new Main()

  implicit val timeout = Timeout(3000 seconds)

  def run[T](adjust: Adjustable): T = {
    val future = main.masterRef ? RunMessage(adjust)
    val resultFuture =
      Await.result(future, timeout.duration).asInstanceOf[Future[Any]]

    Await.result(resultFuture, timeout.duration).asInstanceOf[T]
  }

  def propagate[T]() {
    val future = main.masterRef ? PropagateMessage
    val future2 = Await.result(future, timeout.duration).asInstanceOf[Future[String]]
    Await.result(future2, timeout.duration)
  }

  def put(key: Any, value: Any) {
    val future = main.masterRef ? PutMessage("input", key, value)
    val future2 = Await.result(future, timeout.duration).asInstanceOf[Future[String]]
    Await.result(future2, timeout.duration)
  }

  def load(file: String, max: Int = -1) {
    val xml = scala.xml.XML.loadFile(file)
    var count = 0

    (xml \ "elem").map(elem => {
      (elem \ "key").map(key => {
        (elem \ "value").map(value => {
          if (max > count || max == -1) {
            put(key.text, value.text)
            count += 1
          }
        })
      })
    })
  }

  def update(key: Any, value: Any) {
    val future = main.masterRef ? UpdateMessage("input", key, value)
    val future2 = Await.result(future, timeout.duration).asInstanceOf[Future[String]]
    Await.result(future2, timeout.duration)
  }

  def putMatrix(key: Int, value: Array[Array[Int]]) {
    main.masterRef ! PutMatrixMessage("input", key, value)
  }

  def shutdown() {
    main.masterRef ! ShutdownMessage
  }
}
