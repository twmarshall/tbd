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

import tbd.ListNode
import tbd.messages.{GetArrayMessage, GetListMessage, GetMessage}
import tbd.mod.Mod

class Reader(datastoreRef: ActorRef) {
  def get[T](key: Int): T = {
    implicit val timeout = Timeout(5 seconds)
    val modFuture = datastoreRef ? GetMessage("input", key)
    Await.result(modFuture, timeout.duration)
      .asInstanceOf[T]
  }

  def getArray[T](): Array[T] = {
    implicit val timeout = Timeout(5 seconds)
    val arrayFuture = datastoreRef ? GetArrayMessage("input")
    Await.result(arrayFuture, timeout.duration)
      .asInstanceOf[Array[T]]
  }

  def getList(): Mod[ListNode[String]] = {
    implicit val timeout = Timeout(5 seconds)
    val listFuture = datastoreRef ? GetListMessage("input")
    Await.result(listFuture, timeout.duration)
      .asInstanceOf[Mod[ListNode[String]]]
  }
}
