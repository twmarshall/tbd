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

  implicit val timeout = Timeout(5 seconds)

  def run[T](adjust: Adjustable): Mod[T] = {
    val future = main.masterRef ? RunMessage(adjust)
    val resultFuture =
      Await.result(future, timeout.duration).asInstanceOf[Future[Mod[Any]]]

    Await.result(resultFuture, timeout.duration).asInstanceOf[Mod[T]]
  }

  def propagate[T](): Mod[T] = {
    val future = main.masterRef ? PropagateMessage
    val resultFuture =
      Await.result(future, timeout.duration).asInstanceOf[Future[Mod[Any]]]

    Await.result(resultFuture, timeout.duration).asInstanceOf[Mod[T]]
  }

  def put(key: Int, value: Any) {
    main.masterRef ! PutMessage("input", key, value)
  }

  def putMatrix(key: Int, value: Array[Array[Int]]) {
    main.masterRef ! PutMatrixMessage("input", key, value)
  }

  def shutdown() {
    main.masterRef ! ShutdownMessage
  }
}
