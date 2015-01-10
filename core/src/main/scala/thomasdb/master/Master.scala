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
package thomasdb.master

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.{ask, pipe}
import java.io.File
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import thomasdb.{Adjustable, ThomasDB}
import thomasdb.Constants._
import thomasdb.datastore.Datastore
import thomasdb.messages._
import thomasdb.list._
import thomasdb.stats.Stats
import thomasdb.worker.{Task, Worker}

object Master {
  def props(): Props = Props(classOf[Master])
}

class Master extends Actor with ActorLogging {
  import context.dispatcher

  Stats.registeredWorkers.clear()

  log.info("Master launched.")

  private val workers = Map[Int, ActorRef]()

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
        workerRef: ActorRef, datastoreRef: ActorRef, webuiAddress: String) =>
      log.info("Registering worker at " + workerRef)

      Stats.registeredWorkers += webuiAddress

      val workerId = nextWorkerId
      workers(workerId) = workerRef
      datastoreRefs(workerId) = datastoreRef

      sender ! workerId
      nextWorkerId = incrementWorkerId(nextWorkerId)

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
      (tasks(mutatorId) ? ShutdownTaskMessage) pipeTo sender
      tasks -= mutatorId

      for ((workerId, datastoreRef) <- datastoreRefs) {
        datastoreRef ! ClearMessage()
      }

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
      val input =
        if (conf.double) {
          if (conf.hash) {
            val futures = Buffer[(Future[String], ActorRef, WorkerId)]()

            var index = 0
            for (i <- 1 to conf.partitions) {
              val datastoreRef = datastoreRefs(nextWorker)

              val message = CreateListMessage(conf.copy(partitionIndex = index))
              val future = datastoreRef ? message
              futures += ((future.mapTo[String], datastoreRef, nextWorker))

              cycleWorkers()
              index += 1
            }

            val partitions = Map[WorkerId, Buffer[DoubleListInput[Any, Any]]]()
            for ((future, datastoreRef, workerId) <- futures) {
              val listId = Await.result(future, DURATION)

              if (partitions.contains(workerId)) {
                partitions(workerId) += new DoubleListInput(listId, datastoreRef)
              } else {
                partitions(workerId) = Buffer(new DoubleListInput(listId, datastoreRef))
              }
            }

            new HashPartitionedDoubleListInput(partitions)
          } else {
            val futures = Buffer[(Future[String], ActorRef)]()

            var index = 0
            for (i <- 1 to conf.partitions) {
              val datastoreRef = datastoreRefs(nextWorker)
              cycleWorkers()

              val message = CreateListMessage(conf.copy(partitionIndex = index))
              val future = datastoreRef ? message
              futures += ((future.mapTo[String], datastoreRef))

              index += 1
            }

            val partitions = Buffer[DoubleListInput[Any, Any]]()
            for ((future, datastoreRef) <- futures) {
              val listId = Await.result(future, DURATION)
              partitions += new DoubleListInput(listId, datastoreRef)
            }

            new PartitionedDoubleListInput(partitions)
          }
        } else if (conf.chunkSize == 1) {
          if (conf.partitions == 1) {
            val datastoreRef = datastoreRefs(nextWorker)
            val future = datastoreRef ? CreateListMessage(conf)
            val listId = Await.result(future.mapTo[String], DURATION)
            cycleWorkers()

            new ModListInput(listId, datastoreRef)
          } else {
            val futures = Buffer[(Future[String], ActorRef)]()

            var index = 0
            for (i <- 1 to conf.partitions) {
              val datastoreRef = datastoreRefs(nextWorker)
              cycleWorkers()

              val message = CreateListMessage(conf.copy(partitionIndex = index))
              val future = datastoreRef ? message
              futures += ((future.mapTo[String], datastoreRef))

              index += 1
            }

            val partitions = Buffer[ModListInput[Any, Any]]()
            for ((future, datastoreRef) <- futures) {
              val listId = Await.result(future, DURATION)
              partitions += new ModListInput(listId, datastoreRef)
            }

            new PartitionedModListInput(partitions)
          }
        } else {
          if (conf.partitions == 1) {
            val datastoreRef = datastoreRefs(nextWorker)
            val future = datastoreRef ? CreateListMessage(conf)
            val listId = Await.result(future.mapTo[String], DURATION)
            cycleWorkers()

            new ChunkListInput2(listId, datastoreRef)
          } else {
            val futures = Buffer[(Future[String], ActorRef)]()

            var index = 0
            for (i <- 1 to conf.partitions) {
              val datastoreRef = datastoreRefs(nextWorker)
              cycleWorkers()

              val message = CreateListMessage(conf.copy(partitionIndex = index))
              val future = datastoreRef ? message
              futures += ((future.mapTo[String], datastoreRef))

              index += 1
            }

            val partitions = Buffer[ChunkListInput2[Any, Any]]()
            for ((future, datastoreRef) <- futures) {
              val listId = Await.result(future, DURATION)
              partitions += new ChunkListInput2(listId, datastoreRef)
            }

            new PartitionedChunkListInput2(partitions, conf)
          }
        }

      sender ! input

    case x =>
      log.warning("Master actor received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
