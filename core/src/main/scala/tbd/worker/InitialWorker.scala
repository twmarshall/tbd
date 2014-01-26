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
package tbd.worker

import akka.actor.{Actor, ActorLogging, ActorRef}
import scala.concurrent.Promise

import tbd.{Changeable, TBD}
import tbd.messages._

class InitialWorker[T](id: Int, ddgRef: ActorRef, inputRef: ActorRef)
  extends Actor with ActorLogging {
  log.info("Worker " + id + " launched")

  def receive = {
    case RunTaskMessage(task: Task) => {
      val tbd = new TBD(ddgRef, inputRef, context.system)
      val output = task.func(tbd)
      sender ! output.mod
    }
    case _ => log.warning("InitialWorker " + id + " received unknown message.")
  }
}
