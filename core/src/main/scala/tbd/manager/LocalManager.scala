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
import tbd.mod.Mod
import tbd.mod.ModStore
import tbd.worker.InitialWorker

class LocalManager extends Manager {
  TBD.id += 1
  val system = ActorSystem("masterSystem" + TBD.id,
                           ConfigFactory.load.getConfig("client"))
  val log = Logging(system, "LocalManager")
  val result = Promise[Output]()
  var masterActor: ActorRef = null

  def launch(tbd: TBD): Future[Output] = {
    masterActor = system.actorOf(Props(classOf[Master], tbd, this, result), "masterActor")

    result.future
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
    system.actorOf(Props(classOf[SimpleDDG]), "ddgActor")
  }

  def connectToDDG(): ActorRef = {
    val ddgSelection = system.actorSelection("/user/ddgActor")

    implicit val timeout = Timeout(5 seconds)
    val future = ddgSelection.resolveOne
    Await.result(future, timeout.duration).asInstanceOf[ActorRef]
  }

  def launchInitialWorker(func: () => (Changeable[Any])): ActorRef = {
    system.actorOf(Props(classOf[InitialWorker], func, masterActor), "workerActor")
  }

  def getSystem = system

  def shutdown() {
    log.info("Shutting down.")
    system.shutdown()
    system.awaitTermination()
  }
}
