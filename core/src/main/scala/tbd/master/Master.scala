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
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.mutable.{Map, Set}
import scala.concurrent.duration._
import scala.concurrent.{Await, Future, Promise}

import tbd.{Adjustable, Dest, TBD}
import tbd.datastore.Datastore
import tbd.messages._
import tbd.mod.ModId
import tbd.worker.{Worker, Task}

object Master {
  def props(): Props = Props(classOf[Master])
}

class Master extends Actor with ActorLogging {
  log.info("Master launced.")
  private val datastoreRef = context.actorOf(Datastore.props(), "datastore")
  datastoreRef ! CreateTableMessage("input")

  private var workerRef: ActorRef = null

  private var nextMutatorId = 0

  // Maps mutatorIds to their corresponding workers.
  private val workers = Map[Int, ActorRef]()

  implicit val timeout = Timeout(3000 seconds)

  var result = Promise[Any]
  var resultWaiter: ActorRef = null
  var updateResult = Promise[String]
  var awaiting = 0

  def receive = {
    case RunMessage(adjust: Adjustable, mutatorId: Int) => {
      log.debug("RunMessage")
      result = Promise[Any]

      val workerProps = Worker.props("worker", datastoreRef, self)
      workerRef = context.actorOf(workerProps, "worker" + mutatorId)
      workers(mutatorId) = workerRef

      val resultFuture = workerRef ? RunTaskMessage(new Task((tbd: TBD) => adjust.run(tbd)))

      sender ! resultFuture
    }

    case PropagateMessage => {
      log.info("Master actor initiating change propagation.")
      //log.debug("DDG: {}", Await.result(workerRef ? DDGToStringMessage(""),
      //                                  timeout.duration).asInstanceOf[String])

      result = Promise[Any]
      sender ! result.future

      workerRef ! PropagateMessage
    }

    case FinishedPropagatingMessage => {
      log.debug("Master received FinishedPropagatingMessage.")

      //log.debug("DDG: {}", Await.result(workerRef ? DDGToStringMessage(""),
      //                                  timeout.duration).asInstanceOf[String])

      result.success("okay")
    }

    case PutInputMessage(table: String, key: Any, value: Any) => {
      log.debug("PutInputMessage")
      val future = datastoreRef ? PutMessage(table, key, value, self)

      updateResult = Promise[String]
      sender ! updateResult.future

      assert(awaiting == 0)
      awaiting = Await.result(future, timeout.duration).asInstanceOf[Int]

      if (awaiting == 0) {
        updateResult.success("okay")
      }
    }

    case UpdateInputMessage(table: String, key: Any, value: Any) => {
      log.debug("UpdateInputMessage")
      val future = datastoreRef ? UpdateMessage(table, key, value, self)

      updateResult = Promise[String]
      sender ! updateResult.future

      assert(awaiting == 0)
      awaiting = Await.result(future, timeout.duration).asInstanceOf[Int]

      if (awaiting == 0) {
        updateResult.success("okay")
      }
    }

    case RemoveInputMessage(table: String, key: Any) => {
      log.debug("RemoveInputMessage")
      val future = datastoreRef ? RemoveMessage(table, key, self)

      updateResult = Promise[String]
      sender ! updateResult.future

      assert(awaiting == 0)
      awaiting = Await.result(future, timeout.duration).asInstanceOf[Int]

      if (awaiting == 0) {
        updateResult.success("okay")
      }
    }

    case PebbleMessage(workerRef: ActorRef, modId: ModId, respondTo: ActorRef) => {
      log.debug("Master received PebbleMessage. Sending " +
                "PebblingFinishedMessage(" + modId + ").")
      respondTo ! PebblingFinishedMessage(modId)
    }

    case PebblingFinishedMessage(modId: ModId) => {
      log.debug("Master received PebblingFinishedMessage.")
      assert(awaiting > 0)
      awaiting -= 1
      if (awaiting == 0) {
        updateResult.success("okay")
      }
    }

    case RegisterMutatorMessage => {
      sender ! nextMutatorId
      nextMutatorId += 1
    }

    case GetMutatorDDGMessage(mutatorId: Int) => {
      if (workers.contains(mutatorId)) {
        val future = workers(mutatorId) ? GetDDGMessage
        sender ! Await.result(future, timeout.duration)
      }
    }

    case ShutdownMutatorMessage(mutatorId: Int) => {
      if (workers.contains(mutatorId)) {
        log.debug("Sending CleanupWorkerMessage to " + workers(mutatorId))
        val future = workers(mutatorId) ? CleanupWorkerMessage
        Await.result(future, timeout.duration)

        context.stop(workers(mutatorId))
        workers -= mutatorId
      }

      sender ! "done"
    }

    case x => {
      log.warning("Master actor received unhandled message " +
			            x + " from " + sender + " " + x.getClass)
    }
  }
}
