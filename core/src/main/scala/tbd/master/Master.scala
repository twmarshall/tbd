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
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tbd.{Adjustable, TBD}
import tbd.Constants._
import tbd.datastore.Datastore
import tbd.messages._
import tbd.worker.{Task, Worker}

object Master {
  def props(): Props = Props(classOf[Master])
}

class Master extends Actor with ActorLogging {
  import context.dispatcher

  log.info("Master launced.")

  val datastore = context.actorOf(Datastore.props(), "datastore")

  private var nextMutatorId = 0

  private val workers = Buffer[ActorRef]()

  // Maps mutatorIds to the Worker the mutator's computation was launched on.
  private val mutators = Map[Int, ActorRef]()

  def receive = {
    case GetModMessage(modId: ModId, null) =>
      (datastore ? GetModMessage(modId, null)) pipeTo sender

    case GetMutatorDDGMessage(mutatorId: Int) =>
      (mutators(mutatorId) ? GetMutatorDDGMessage(mutatorId)) pipeTo sender

    case PropagateMutatorMessage(mutatorId: Int) =>
      log.info("Initiating change propagation for mutator " + mutatorId)

      (workers(0) ? PropagateMutatorMessage(mutatorId)) pipeTo sender

    case RegisterMutatorMessage =>
      log.info("Registering mutator " + nextMutatorId)
      sender ! nextMutatorId
      nextMutatorId += 1

    case RegisterWorkerMessage(worker: ActorRef) =>
      log.info("Registering worker at " + worker)
      workers += worker
      sender ! datastore

    case RunMutatorMessage(adjust: Adjustable[_], mutatorId: Int) =>
      log.info("Starting initial run for mutator " + mutatorId)

      mutators(mutatorId) = workers(0)
      (workers(0) ? RunMutatorMessage(adjust, mutatorId)) pipeTo sender

    case ShutdownMutatorMessage(mutatorId: Int) =>
      log.info("Shutting down mutator " + mutatorId)
      (mutators(mutatorId) ? ShutdownMutatorMessage(mutatorId)) pipeTo sender

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      (datastore ? UpdateModMessage(modId, value, null)) pipeTo sender

    case UpdateModMessage(modId: ModId, null, null) =>
      (datastore ? UpdateModMessage(modId, null, null)) pipeTo sender

    case x =>
      log.warning("Master actor received unhandled message " +
		  x + " from " + sender + " " + x.getClass)
  }
}
