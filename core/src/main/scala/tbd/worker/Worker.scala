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
import akka.pattern.{ask, pipe}
import scala.collection.mutable.Map
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

import tbd.Adjustable
import tbd.Constants._
import tbd.messages._

object Worker {
  def props(masterRef: ActorRef) = Props(classOf[Worker], masterRef)
}

class Worker(masterRef: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher

  private val datastore = Await.result(
    (masterRef ? RegisterWorkerMessage(self)).mapTo[ActorRef], DURATION)

  def receive = {
    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      sender ! "done"

    case ScheduleTaskMessage(id: String, parent: ActorRef) =>
      log.debug("Scheduling task " + id)
      val taskProps = Task.props(id, parent, datastore, masterRef)
      val taskRef = context.actorOf(taskProps, id)

      sender ! taskRef

    case x => println("Worker received unhandled message " + x)
  }
}
