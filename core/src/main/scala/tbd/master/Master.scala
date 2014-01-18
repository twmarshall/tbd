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
import scala.concurrent.Promise

import tbd.{Changeable, Dest, TBD}
import tbd.ddg.SimpleDDG
import tbd.manager.Manager
import tbd.messages._
import tbd.mod.{Mod, ModStore}
import tbd.worker.{InitialWorker, Task}

class Master(manager: Manager)
    extends Actor with ActorLogging {
  log.info("Master launced.")
  val ddgRef = context.actorOf(Props(classOf[SimpleDDG]), "ddgActor")
  var i = 0

  def runTask[T](tbd: TBD): Promise[Any] = {
    i += 1
    val resultPromise = Promise[Any]()
    val workerActor = context.actorOf(Props(classOf[InitialWorker[T]], resultPromise, i),
				    "workerActor" + i)
    tbd.initialize(manager, ddgRef)
    workerActor ! RunTaskMessage(new Task(() => tbd.run(new Dest)))
    resultPromise
  }

  def receive = {
    case RunMessage(tbd: TBD) => {
      sender ! runTask(tbd).future
    }
    case _ => log.warning("Master actor received unkown message!")
  }
}
