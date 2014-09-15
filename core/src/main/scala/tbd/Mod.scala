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
import java.io.Serializable
import scala.collection.mutable.{ArrayBuffer, Set}
import scala.concurrent.{Await, Future, Lock, Promise}

import tbd.Constants._
import tbd.datastore.Datastore
import tbd.master.Main
import tbd.messages._

class Mod[T] extends Serializable {
  val idFuture = Datastore.newModId()
  lazy val id = Await.result(idFuture, DURATION)

  var value: T = null.asInstanceOf[T]

  def read(workerRef: ActorRef = null): T = {
    if (workerRef != null) {
      Datastore.addDependency(id, workerRef)
    }
    value
  }

  def update(_value: T): ArrayBuffer[Future[String]] = {
    val futures = Datastore.updateMod(id, _value)
    value = _value
    futures
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[Mod[T]]) {
      false
    } else {
      val that = obj.asInstanceOf[Mod[T]]
      that.id == id
    }
  }

  //Notice: Removed reading of mod from toString, because calling
  //read() when the mod is no longer valid (for instance in Visualizer)
  //causes a crash.
  override def toString = "Mod(" + id + ")"

  override def hashCode() = id.hashCode()
}
