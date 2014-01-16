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

import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.TBD
import tbd.ListNode
import tbd.manager.Manager
import tbd.messages._
import tbd.mod.Mod

class Reader(manager: Manager) {
  val inputActor = manager.connectToInput()

  def get(key: Int): Mod[String] = {
    implicit val timeout = Timeout(5 seconds)
    val sizeFuture = inputActor ? GetMessage(key)
    Await.result(sizeFuture, timeout.duration)
      .asInstanceOf[Mod[String]]
  }

  def getArray(): Array[Mod[String]] = {
    implicit val timeout = Timeout(5 seconds)
    val sizeFuture = inputActor ? GetArrayMessage
    Await.result(sizeFuture, timeout.duration)
      .asInstanceOf[Array[Mod[String]]]
  }

  def getList(): Mod[ListNode[String]] = {
    implicit val timeout = Timeout(5 seconds)
    val sizeFuture = inputActor ? GetListMessage
    Await.result(sizeFuture, timeout.duration)
      .asInstanceOf[Mod[ListNode[String]]]
  }
}
