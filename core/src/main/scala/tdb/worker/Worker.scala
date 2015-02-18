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
package tdb.worker

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import scala.collection.mutable.Map
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

import tdb.Adjustable
import tdb.Constants._
import tdb.datastore.DatastoreActor
import tdb.messages._

object Worker {
  def props
      (masterRef: ActorRef,
       conf: WorkerConf,
       systemURL: String,
       webuiAddress: String) =
    Props(classOf[Worker], masterRef, conf,
          systemURL, webuiAddress)
}

class Worker
    (masterRef: ActorRef,
     conf: WorkerConf,
     systemURL: String,
     webuiAddress: String) extends Actor with ActorLogging {
  import context.dispatcher

  log.info("Worker launched.")

  private val datastore = context.actorOf(
    DatastoreActor.props(conf), "datastore")

  private val workerId = {
    val message = RegisterWorkerMessage(
      systemURL + "/user/worker",
      systemURL + "/user/worker/datastore",
      webuiAddress)
    Await.result((masterRef ? message).mapTo[WorkerId], DURATION)
  }

  datastore ! SetIdMessage(workerId)

  // A unique id to assign to tasks forked from this context. The datastore
  // has TaskId = 0 for the purpose of creating ModIds.
  private var nextTaskId: TaskId = 1

  def receive = {
    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      sender ! "done"

    case CreateTaskMessage(parent: ActorRef) =>
      var taskId = workerId.toInt
      taskId = (taskId << 16) + nextTaskId

      val taskProps = Task.props(taskId, parent, datastore, masterRef)
      val taskRef = context.actorOf(taskProps, taskId + "")

      sender ! taskRef

      nextTaskId += 1

    case CreateModMessage(value: Any) =>
      (datastore ? CreateModMessage(value)) pipeTo sender

    case CreateModMessage(null) =>
      (datastore ? CreateModMessage(null)) pipeTo sender

    case "started" => sender ! "done"

    case x => println("Worker received unhandled message " + x)
  }
}
