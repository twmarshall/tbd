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
import scala.collection.mutable.Set
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

  private var i = 0

  implicit val timeout = Timeout(3000 seconds)

  var result = Promise[Any]
  var resultWaiter: ActorRef = null
  var updateResult = Promise[String]
  var awaiting = 0

  private def runTask(adjust: Adjustable): Future[Any] = {
    i += 1
    val workerProps = Worker.props("worker" + i, datastoreRef, self)
    workerRef = context.actorOf(workerProps, "worker" + i)
    workerRef ? RunTaskMessage(new Task((tbd: TBD) => adjust.run(tbd)))
  }

  def receive = {
    case RunMessage(adjust: Adjustable) => {
      log.debug("RunMessage")
      result = Promise[Any]
      sender ! runTask(adjust)
    }

    case PropagateMessage => {
      log.info("Master actor initiating change propagation.")
      log.debug("DDG: {}", Await.result(workerRef ? DDGToStringMessage(""),
					                              timeout.duration).asInstanceOf[String])

      result = Promise[Any]
      sender ! result.future

      workerRef ! PropagateMessage
    }

    case FinishedPropagatingMessage => {
      log.debug("Master received FinishedPropagatingMessage.")

      log.debug("DDG: {}", Await.result(workerRef ? DDGToStringMessage(""),
					                              timeout.duration).asInstanceOf[String])

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
      log.debug("UpdateMessage " + awaiting)

      if (awaiting == 0) {
        updateResult.success("okay")
      }
    }

    /*case PutMatrixMessage(
        table: String,
        key: Any,
        value: Array[Array[Int]]) => {
      val future = datastoreRef ? PutMatrixMessage(table, key, value)
      sender ! Await.result(future, timeout.duration)
    }*/

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

    case ShutdownMessage => {
      log.info("Master shutting down.")
      context.system.shutdown()
    }

    case x => {
      log.warning("Master actor received unhandled message " +
			            x + " from " + sender + " " + x.getClass)
    }
  }
}
