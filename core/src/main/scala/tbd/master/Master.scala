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

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tbd.{Adjustable, TBD}
import tbd.Constants._
import tbd.messages._
import tbd.worker.Task

object Master {
  def props(datastore: ActorRef): Props = Props(classOf[Master], datastore)
}

class Master(val datastore: ActorRef) extends Actor with ActorLogging {
  import context.dispatcher
  log.info("Master launced.")

  private var taskRef: ActorRef = null

  private var nextMutatorId = 0

  // Maps mutatorIds to their corresponding tasks.
  private val tasks = Map[Int, ActorRef]()

  def receive = {
    case RunMessage(adjust: Adjustable[_], mutatorId: Int) => {
      log.debug("RunMessage")

      val taskProps = Task.props("w0", self, datastore)
      taskRef = context.actorOf(taskProps, "task" + mutatorId)
      tasks(mutatorId) = taskRef

      val resultFuture = taskRef ? RunTaskMessage(adjust)

      sender ! resultFuture
    }

    case PropagateMessage => {
      log.info("Master actor initiating change propagation.")

      val future = taskRef ? PropagateMessage
      val respondTo = sender
      future.onComplete((_try: Try[Any]) => {
	//log.debug("DDG: {}\n\n", Await.result(taskRef ? DDGToStringMessage(""),
        //                                      DURATION).asInstanceOf[String])
        respondTo ! "done"
      })
    }

    case PebbleMessage(taskRef: ActorRef, modId: ModId, finished: Promise[String]) => {
      finished.success("done")
    }

    case RegisterMutatorMessage => {
      sender ! nextMutatorId
      nextMutatorId += 1
    }

    case GetMutatorDDGMessage(mutatorId: Int) => {
      if (tasks.contains(mutatorId)) {
        val future = tasks(mutatorId) ? GetDDGMessage
        sender ! Await.result(future, DURATION)
      }
    }

    case ShutdownMutatorMessage(mutatorId: Int) => {
      if (tasks.contains(mutatorId)) {
        log.debug("Sending CleanupTaskMessage to " + tasks(mutatorId))
        val future = tasks(mutatorId) ? CleanupTaskMessage
        Await.result(future, DURATION)

        context.stop(tasks(mutatorId))
        tasks -= mutatorId
      }

      sender ! "done"
    }

    case CleanupMessage => {
      sender ! "done"
    }

    case RegisterWorkerMessage(worker: ActorRef) => {
      println(worker)
    }

    case x => {
      log.warning("Master actor received unhandled message " +
		  x + " from " + sender + " " + x.getClass)
    }
  }
}
