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
package tbd.manager

import akka.actor.{Actor, ActorRef, ActorSystem, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.collection.mutable.Map
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.Future
import scala.concurrent.Promise

import tbd.Changeable
import tbd.Output
import tbd.TBD
import tbd.ddg.SimpleDDG
import tbd.input.Input
import tbd.master.Master
import tbd.messages.RunTaskMessage
import tbd.mod.Mod
import tbd.mod.ModStore
import tbd.worker.{InitialWorker, Task}

class LocalManager extends Manager {
  TBD.id += 1
  val system = ActorSystem("masterSystem" + TBD.id,
                           ConfigFactory.load.getConfig("client"))
  val log = Logging(system, "LocalManager")
  var masterActor: ActorRef = null

  def launch(): ActorRef = {
    masterActor = system.actorOf(Props(classOf[Master], this), "masterActor")

    masterActor
  }

  def launchInput() {
    system.actorOf(Props(classOf[Input], this), "input")
  }

  def connectToInput(): ActorRef = {
    val ddgSelection = system.actorSelection("/user/input")

    implicit val timeout = Timeout(5 seconds)
    val future = ddgSelection.resolveOne
    Await.result(future, timeout.duration).asInstanceOf[ActorRef]
  }

  def launchModStore() {
    system.actorOf(Props(classOf[ModStore]), "modStore")
  }

  def connectToModStore(): ActorRef = {
    val modStoreSelection = system.actorSelection("/user/modStore")

    implicit val timeout = Timeout(5 seconds)
    val future = modStoreSelection.resolveOne
    Await.result(future, timeout.duration).asInstanceOf[ActorRef]
  }

  def launchDDG() {

  }

  def connectToDDG(): ActorRef = {
    val ddgSelection = system.actorSelection("/user/masterActor/ddgActor")

    implicit val timeout = Timeout(5 seconds)
    val future = ddgSelection.resolveOne
    Await.result(future, timeout.duration).asInstanceOf[ActorRef]
  }

  var i = 0
  def launchInitialWorker[T](func: () => (Changeable[T])): Promise[T] = {
    i += 1
    val result = Promise[T]()
    val ref = system.actorOf(Props(classOf[InitialWorker[T]], result, i),
		   "workerActor" + i)
    ref ! RunTaskMessage(new Task(func.asInstanceOf[() => (Changeable[Any])]))
    result
  }

  def getSystem = system

  def shutdown() {
    log.info("Shutting down.")
    system.shutdown()
    system.awaitTermination()
  }
}
