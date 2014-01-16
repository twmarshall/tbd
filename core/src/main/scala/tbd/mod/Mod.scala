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
package tbd.mod

import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.manager.Manager
import tbd.messages._

class Mod[T](newValue: T, manager: Manager) {
  val modStoreRef = manager.connectToModStore()

  implicit val timeout = Timeout(5 seconds)

  val writeFuture = if (newValue == null) {
    modStoreRef ? WriteNullModMessage
  } else {
    modStoreRef ? WriteModMessage(newValue)
  }

  val id = Await.result(writeFuture, timeout.duration)
    .asInstanceOf[ModId]

  def read(): T = {
    implicit val timeout = Timeout(5 seconds)
    val readFuture = modStoreRef ? ReadModMessage(id)
    val ret = Await.result(readFuture, timeout.duration)

    ret match {
      case NullValueMessage => null.asInstanceOf[T]
      case _ => ret.asInstanceOf[T]
    }
  }

  override def toString: String =
    read().toString
}
