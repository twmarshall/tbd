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
import scala.concurrent.duration._
import scala.concurrent.Future

import tbd.{Adjustable, Dest, TBD}
import tbd.datastore.Datastore
import tbd.messages._
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

  private def runTask[T](adjust: Adjustable): Future[Any] = {
    i += 1
    val workerProps = Worker.props[T](i, datastoreRef)
    workerRef = context.actorOf(workerProps, "workerActor" + i)
    implicit val timeout = Timeout(5 seconds)
    workerRef ? RunTaskMessage(new Task((tbd: TBD) => adjust.run(new Dest, tbd)))
  }

  private def propagate(): Future[Any] = {
    log.info("Master actor initiating change propagation.")
    implicit val timeout = Timeout(5 seconds)
    workerRef ? PropagateMessage
  }

  def receive = {
    case RunMessage(adjust: Adjustable) => {
      sender ! runTask(adjust)
    }
    case PropagateMessage => {
      sender ! propagate()
    }
    case ShutdownMessage => {
      context.system.shutdown()
      context.system.awaitTermination()
    }
    case x => log.warning("Master actor received unhandled message " +
			  x + " from " + sender)
  }
}
