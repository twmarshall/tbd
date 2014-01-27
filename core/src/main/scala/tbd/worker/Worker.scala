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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}

import tbd.TBD
import tbd.messages.RunTaskMessage

object Worker {
  def props[T](id: Int, ddgRef: ActorRef, datastoreRef: ActorRef): Props =
    Props(classOf[Worker[T]], id, ddgRef, datastoreRef)
}

class Worker[T](id: Int, ddgRef: ActorRef, datastoreRef: ActorRef)
  extends Actor with ActorLogging {
  log.info("Worker " + id + " launched")

  def receive = {
    case RunTaskMessage(task: Task) => {
      val tbd = new TBD(ddgRef, datastoreRef, context.system)
      val output = task.func(tbd)
      sender ! output.mod
    }
    case x => log.warning("Worker" + id + " actor received unhandled message " +
			  x + " from " + sender)
  }
}
