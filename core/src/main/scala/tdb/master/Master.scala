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
package tdb.master

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import java.io.File
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tdb.{Adjustable, TDB}
import tdb.Constants._
import tdb.datastore.Datastore
import tdb.messages._
import tdb.list._
import tdb.stats.{Stats, WorkerInfo}
import tdb.util._
import tdb.worker.{Task, Worker}

object Master {
  def props(): Props = Props(classOf[Master])
}

class Master extends Actor with ActorLogging {
  import context.dispatcher

  Stats.registeredWorkers.clear()

  log.info("Master launched.")

  private val workers = Map[WorkerId, ActorRef]()

  // Maps workerIds to Datastores.
  private val datastoreRefs = Map[WorkerId, ActorRef]()

  // Maps mutatorIds to the Task the mutator's computation was launched on.
  private val tasks = Map[Int, ActorRef]()

  private var nextMutatorId = 0

  private var nextWorkerId: WorkerId = 0

  // The next Worker to schedule a Task on.
  private var nextWorker: WorkerId = 0

  def cycleWorkers() {
    nextWorker = (incrementWorkerId(nextWorker) % workers.size).toShort
  }

  def receive = {
    // Worker
    case RegisterWorkerMessage(
        worker: String, datastore: String, webuiAddress: String) =>
      val workerId = nextWorkerId
      sender ! workerId

      val workerRef = AkkaUtil.getActorFromURL(worker, context.system)
      val datastoreRef = AkkaUtil.getActorFromURL(datastore, context.system)

      log.info("Registering worker at " + workerRef)

      workers(workerId) = workerRef
      datastoreRefs(workerId) = datastoreRef

      nextWorkerId = incrementWorkerId(nextWorkerId)

      Stats.registeredWorkers += WorkerInfo(workerId, webuiAddress)

      for ((thatWorkerId, thatDatastoreRef) <- datastoreRefs) {
        thatDatastoreRef ! RegisterDatastoreMessage(workerId, datastoreRef)
        datastoreRef ! RegisterDatastoreMessage(thatWorkerId, thatDatastoreRef)
      }

    case ScheduleTaskMessage(parent: ActorRef, workerId: WorkerId) =>
      if (workerId == -1) {
        (workers(nextWorker) ? CreateTaskMessage(parent)) pipeTo sender
        cycleWorkers()
      } else {
        (workers(workerId) ? CreateTaskMessage(parent)) pipeTo sender
      }

    // Mutator
    case RegisterMutatorMessage =>
      log.info("Registering mutator " + nextMutatorId)
      sender ! nextMutatorId
      nextMutatorId += 1

    case RunMutatorMessage(adjust: Adjustable[_], mutatorId: Int) =>
      log.info("Starting initial run for mutator " + mutatorId)

      val taskRefFuture = workers(nextWorker) ?
        CreateTaskMessage(workers(nextWorker))
      cycleWorkers()
      val taskRef = Await.result(taskRefFuture.mapTo[ActorRef], DURATION)

      (taskRef ? RunTaskMessage(adjust)) pipeTo sender

      tasks(mutatorId) = taskRef

    case PropagateMutatorMessage(mutatorId: Int) =>
      log.info("Initiating change propagation for mutator " + mutatorId)

      (tasks(mutatorId) ? PropagateTaskMessage) pipeTo sender

    case GetMutatorDDGMessage(mutatorId: Int) =>
      (tasks(mutatorId) ? GetTaskDDGMessage) pipeTo sender

    case ShutdownMutatorMessage(mutatorId: Int) =>
      log.info("Shutting down mutator " + mutatorId)

      for ((workerId, workerRef) <- workers) {
        workerRef ! ClearMessage
      }

      Stats.clear()
      Await.result(tasks(mutatorId) ? ClearModsMessage, DURATION)

      val f = tasks(mutatorId) ? ShutdownTaskMessage
      tasks -= mutatorId

      Await.result(f, DURATION)
      for ((workerId, datastoreRef) <- datastoreRefs) {
        datastoreRef ! ClearMessage()
      }

      sender ! "done"

    // Datastore
    case CreateModMessage(value: Any) =>
      (workers(nextWorker) ? CreateModMessage(value)) pipeTo sender
      cycleWorkers()

    case CreateModMessage(null) =>
      (workers(nextWorker) ? CreateModMessage(null)) pipeTo sender
      cycleWorkers()

    case GetModMessage(modId: ModId, null) =>
      val workerId = getWorkerId(modId)
      (datastoreRefs(workerId) ? GetModMessage(modId, null)) pipeTo sender

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      val workerId = getWorkerId(modId)
      (datastoreRefs(workerId) ? UpdateModMessage(modId, value, null)) pipeTo sender

    case UpdateModMessage(modId: ModId, null, null) =>
      val workerId = getWorkerId(modId)
      (datastoreRefs(workerId) ? UpdateModMessage(modId, null, null)) pipeTo sender

    case CreateListMessage(conf: ListConf) =>
      val futures = Buffer[Future[ObjHasher[(String, ActorRef)]]]()

      var index = 0
      for ((workerId, workerRef) <- workers) {
        val future = {
          val message = CreateListIdsMessage(conf, index, workers.size)
          datastoreRefs(workerId) ? message
        }

        futures += future.mapTo[ObjHasher[(String, ActorRef)]]
        index += 1
      }

      var hasher: ObjHasher[(String, ActorRef)] = null
      Await.result(Future.sequence(futures), DURATION).foreach {
        case thisHasher =>
          if (hasher == null) {
            hasher = thisHasher
          } else {
            hasher = ObjHasher.combineHashers(hasher, thisHasher)
          }
      }
      assert(hasher.isComplete())

      val input = conf match {
        case aggregatorConf: AggregatorListConf[_] =>
          new AggregatorInput(hasher, aggregatorConf)

        case SimpleListConf(_, _, 1, _, false, _) =>
          new HashPartitionedDoubleListInput(hasher)

        case SimpleListConf(_, _, _, _, false, _) =>
          new HashPartitionedDoubleChunkListInput(hasher, conf)
        case columnConf: ColumnListConf =>
          new ColumnListInput(hasher, columnConf)
        case _ => ???
      }

      sender ! input

    case LoadFileMessage(fileName: String, partitions: Int) =>
      val futures = Buffer[Future[Any]]()

      var index = 0
      for ((workerId, datastoreRef) <- datastoreRefs) {
        val message = LoadPartitionsMessage(
          fileName, workers.size, index, partitions)
        futures += (datastoreRef ? message)
        index += 1
      }

      Await.result(Future.sequence(futures), DURATION)

      sender ! "done"

    case x =>
      log.warning("Master actor received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
