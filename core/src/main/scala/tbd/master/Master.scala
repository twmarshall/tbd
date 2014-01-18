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
package tbd.master

import akka.actor.{Actor, ActorRef, ActorLogging, ActorSystem, Props}
import akka.pattern.ask
import akka.util.Timeout
import com.typesafe.config.ConfigFactory
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.{Future, Promise}

import tbd.{Adjustable, Changeable, Dest, TBD}
import tbd.ddg.SimpleDDG
import tbd.input.Input
import tbd.messages._
import tbd.mod.{Mod, ModStore}
import tbd.worker.{InitialWorker, Task}

class Master extends Actor with ActorLogging {
  log.info("Master launced.")
  val ddgRef = context.actorOf(Props(classOf[SimpleDDG]), "ddgActor")

  val modStoreSelection = context.actorSelection("/user/modStore")
  implicit val timeout = Timeout(5 seconds)
  val modStoreFuture = modStoreSelection.resolveOne
  val modStoreRef = Await.result(modStoreFuture, timeout.duration).asInstanceOf[ActorRef]

  val inputSelection = context.actorSelection("/user/input")
  val inputFuture = inputSelection.resolveOne
  val inputRef = Await.result(inputFuture, timeout.duration).asInstanceOf[ActorRef]

  var i = 0

  def runTask[T](adjust: Adjustable): Future[Any] = {
    i += 1
    val workerRef = context.actorOf(Props(classOf[InitialWorker[T]], i, ddgRef, inputRef, modStoreRef),
				    "workerActor" + i)
    workerRef ? RunTaskMessage(new Task((tbd: TBD) => adjust.run(new Dest, tbd)))
  }

  def receive = {
    case RunMessage(adjust: Adjustable) => {
      sender ! runTask(adjust)
    }
    case ShutdownMessage => {
      context.system.shutdown()
      context.system.awaitTermination()
    }
    case _ => log.warning("Master actor received unkown message!")
  }
}
