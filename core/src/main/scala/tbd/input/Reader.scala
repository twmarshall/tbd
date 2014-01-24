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

class Reader(inputRef: ActorRef) {

  def get[T](key: Int): Mod[T] = {
    implicit val timeout = Timeout(5 seconds)
    val modFuture = inputRef ? GetMessage(key)
    Await.result(modFuture, timeout.duration)
      .asInstanceOf[Mod[T]]
  }

  def getArray(): Array[Mod[String]] = {
    implicit val timeout = Timeout(5 seconds)
    val arrayFuture = inputRef ? GetArrayMessage
    Await.result(arrayFuture, timeout.duration)
      .asInstanceOf[Array[Mod[String]]]
  }

  def getList(): Mod[ListNode[String]] = {
    implicit val timeout = Timeout(5 seconds)
    val listFuture = inputRef ? GetListMessage
    Await.result(listFuture, timeout.duration)
      .asInstanceOf[Mod[ListNode[String]]]
  }
}
