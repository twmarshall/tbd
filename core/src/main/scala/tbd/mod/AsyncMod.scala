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

import scala.concurrent.{Lock, Future}
import tbd.Constants._
import tbd.messages._

import akka.actor.ActorRef
import scala.collection.mutable.{ArrayBuffer, Set}

class AsyncMod[T](id: ModId) extends Mod[T](id, null.asInstanceOf[T]) {

  val readyLock = new Lock()
  var dataReady: Boolean = false

  invalidate()

  def invalidate() {
    println("AsyncMod invalidating... @" + Thread.currentThread().getId())
    readyLock.acquire()
    dataReady = false
    println("done. @" + Thread.currentThread().getId())
  }

  private def validate() {
    dataReady = true
    readyLock.release()
  }

  override def read(workerRef: ActorRef = null): T = {
    //Note: Could get additional speedup with dirty locking
    println("AsyncMod reading... @" + Thread.currentThread().getId())
    readyLock.acquire()
    readyLock.release()
    println("done. @" + Thread.currentThread().getId())

    super.read(workerRef)
  }

  override def update(_value: T): ArrayBuffer[Future[String]] = {
    println("AsyncMod updating... @" + Thread.currentThread().getId())
    if(dataReady) {
      throw new IllegalStateException("A lazy Mod has to be invalidated " +
                                      "before the update operation starts.")
    }

    validate()
    println("done. @" + Thread.currentThread().getId())

    super.update(_value)
  }
}
