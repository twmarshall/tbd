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
import scala.collection.mutable.Buffer
import scala.concurrent.{Await, Future}

import tbd.Constants._
import tbd.datastore.Datastore
import tbd.ddg.DDG
import tbd.master.Main
import tbd.messages._

class Mutator(aMain: Main = null) {
  import scala.concurrent.ExecutionContext.Implicits.global

  var launchedMain = false

  val futures = Buffer[Future[String]]()

  val main =
    if (aMain == null) {
      launchedMain = true
      new Main()
    } else {
      aMain
    }

  val idFuture = main.masterRef ? RegisterMutatorMessage
  val id = Await.result(idFuture, DURATION).asInstanceOf[Int]

  var nextModId = 0
  def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T]("d." + nextModId)
    nextModId += 1
    futures += mod.update(value)
    mod
  }

  def updateMod[T](mod: Mod[T], value: T) {
    futures += mod.update(value)
  }

  def run[T](adjust: Adjustable[T]): T = {
    Await.result(Future.sequence(futures), DURATION)
    futures.clear()

    val future = main.masterRef ? RunMessage(adjust, id)
    val resultFuture =
      Await.result(future, DURATION).asInstanceOf[Future[Any]]

    Await.result(resultFuture, DURATION).asInstanceOf[T]
  }

  def propagate() {
    Await.result(Future.sequence(futures), DURATION)
    futures.clear()

    val future = main.masterRef ? PropagateMessage
    Await.result(future, DURATION)
  }

  def getDDG(): DDG  = {
    val ddgFuture = main.masterRef ? GetMutatorDDGMessage(id)
    Await.result(ddgFuture, DURATION).asInstanceOf[DDG]
  }

  def shutdown() {
    if (launchedMain) {
      main.shutdown()
    } else {
      Await.result(main.masterRef ? ShutdownMutatorMessage(id), DURATION)
    }
  }
}
