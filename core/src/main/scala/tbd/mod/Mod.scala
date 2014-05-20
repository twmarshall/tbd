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

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future, Promise}

import tbd.Constants._
import tbd.TBD
import tbd.messages._

class Mod[T](datastoreRef: ActorRef, _id: ModId) {
  val id = _id

  def read(workerRef: ActorRef = null): T = {
    val ret =
      if (workerRef != null) {
        val readPromise = datastoreRef ? ReadModMessage(id, workerRef)
        Await.result(readPromise, DURATION)
      } else {
        val readPromise = datastoreRef ? GetMessage("mods", id)
        Await.result(readPromise, DURATION)
      }

    ret match {
      case NullMessage => null.asInstanceOf[T]
      case _ => ret.asInstanceOf[T]
    }
  }

  def update(value: T): ArrayBuffer[Future[String]] = {
    val future = datastoreRef ? UpdateModMessage(id, value)
    Await.result(future, DURATION).asInstanceOf[ArrayBuffer[Future[String]]]
  }

  override def toString = {
    val value = read()
    if (value == null) {
      "null"
    } else {
      value.toString
    }
  }
}
