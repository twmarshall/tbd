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

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.master.Main
import tbd.messages._
import tbd.worker.Worker

class Dest[T](datastoreRef: ActorRef) {
  val modFuture = datastoreRef ? CreateModMessage(null)
  val mod = Await.result(modFuture.mapTo[Mod[T]], DURATION)

  override def toString(): String = {
    "Dest(" + mod.id + ")"
  }
}
