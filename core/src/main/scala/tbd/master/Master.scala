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

import akka.actor.{Actor, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import scala.concurrent.duration._
import scala.concurrent.Future

import tbd.{Adjustable, Dest, TBD}
import tbd.ddg.SimpleDDG
import tbd.input.Input
import tbd.messages._
import tbd.mod.ModStore
import tbd.worker.{InitialWorker, Task}

class Master extends Actor with ActorLogging {
  log.info("Master launced.")
  val ddgRef = context.actorOf(Props(classOf[SimpleDDG]), "ddgActor")

  val modStoreRef = context.actorOf(Props(classOf[ModStore]), "modStore")
  val inputRef = context.actorOf(Props(classOf[Input], modStoreRef), "input")

  var adjustable: Adjustable = null

  var i = 0

  def runTask[T](adjust: Adjustable): Future[Any] = {
    i += 1
    val workerRef = context.actorOf(Props(classOf[InitialWorker[T]], i, ddgRef, inputRef, modStoreRef),
				    "workerActor" + i)
    implicit val timeout = Timeout(5 seconds)
    workerRef ? RunTaskMessage(new Task((tbd: TBD) => adjust.run(new Dest, tbd)))
  }

  def receive = {
    case RunMessage(adjust: Adjustable) => {
      adjustable = adjust
      sender ! runTask(adjust)
    }
    case PropagateMessage => {
      sender ! runTask(adjustable)
    }
    case ShutdownMessage => {
      context.system.shutdown()
      context.system.awaitTermination()
    }
    case _ => log.warning("Master actor received unkown message!")
  }
}
