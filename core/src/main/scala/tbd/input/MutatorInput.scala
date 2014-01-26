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
package tbd.input

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.messages.{PutMessage, PutMatrixMessage, PutModMessage}
import tbd.mod.Mod

class MutatorInput(inputRef: ActorRef) {
  implicit val timeout = Timeout(5 seconds)

  def get(key: Int): Any = {
    "asdf"
  }

  def put(key: Int, value: Any) {
    inputRef ! PutMessage("input", key, value)
  }

  def putMod(key: Int, value: Any): Mod[Any] = {
    val retFuture =
      inputRef ? PutModMessage("input", key, value)
    Await.result(retFuture, timeout.duration).asInstanceOf[Mod[Any]]
  }

  def putMatrix(key: Int, value: Array[Array[Int]]) {
    inputRef ! PutMatrixMessage("input", key, value)
  }
}
