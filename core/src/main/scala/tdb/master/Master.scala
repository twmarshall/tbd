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

import akka.actor.{Actor, ActorLogging, ActorRef, Props, Terminated}
import akka.pattern.{ask, pipe}
import java.io.File
import scala.collection.mutable
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future, Promise}
import scala.util.{Failure, Success, Try}

import tdb.{Adjustable, TDB}
import tdb.Constants._
import tdb.datastore.{Datastore, ModifierActor}
import tdb.messages._
import tdb.list._
import tdb.stats.Stats
import tdb.util._
import tdb.worker.{Task, Worker, WorkerInfo}

object Master {
  def props(): Props = Props(classOf[Master])
}

class Master extends Actor with ActorLogging {
  import context.dispatcher

  Stats.registeredWorkers.clear()

  log.info("Master launched.")

  private val scheduler = new Scheduler()

  private val workers = Map[WorkerId, ActorRef]()
  private val workerInfos = Map[WorkerId, WorkerInfo]()

  private val tasks = mutable.Set[TaskInfo]()

  private val datastores = Map[TaskId, DatastoreInfo]()

  // Maps mutatorIds to the Task the mutator's computation was launched on.
  private val rootTasks = Map[Int, ActorRef]()

  private var nextMutatorId = 0

  private var nextWorkerId: WorkerId = 0

  private var nextTaskId: TaskId = 1

  // The next Worker to schedule a Task on.
  private var nextWorker: WorkerId = 0

  // The total number of cores reported by all registered Workers.
  private var totalCores = 0

  private var nextInputId: InputId = 0

  private def createPartitions
      (workerId: WorkerId,
       workerRef: ActorRef,
       listConf: ListConf,
       workerIndex: Int) = {
    val numWorkers = workers.size

    val partitionsPerWorker = listConf.partitions / numWorkers
    val partitions =
      if (workerIndex == numWorkers - 1 &&
          listConf.partitions % numWorkers != 0) {
        numWorkers % partitionsPerWorker
      } else {
        partitionsPerWorker
      }

    val newDatastores = Map[TaskId, ActorRef]()
    var hasher: ObjHasher[ActorRef] = null
    for (i <- 0 until partitions) {
      val start = workerIndex * partitionsPerWorker + i
      val thisRange = new HashRange(start, start + 1, listConf.partitions)
      val datastoreId = nextTaskId
      nextTaskId += 1

      val modifierRef = Await.result(
        (workerRef ? CreateDatastoreMessage(
          listConf, datastoreId, thisRange)).mapTo[ActorRef],
        DURATION)

      val thisHasher = ObjHasher.makeHasher(thisRange, modifierRef)
      if (hasher == null) {
        hasher = thisHasher
      } else {
        hasher = ObjHasher.combineHashers(thisHasher, hasher)
      }

      val datastoreInfo = new DatastoreInfo(
        datastoreId, modifierRef, listConf, workerId, thisRange)
      datastores(datastoreId) = datastoreInfo
    }
    hasher
  }

  private def launchMainDatastore(workerId: WorkerId) {
    val datastoreId = workerInfos(workerId).mainDatastoreId
    val workerRef = workers(workerId)

    val message = CreateDatastoreMessage(null, datastoreId, null)
    val datastoreRef =
      Await.result((workerRef ? message).mapTo[ActorRef], DURATION)

    val datastoreInfo = new DatastoreInfo(
      datastoreId, datastoreRef, null, workerId, null)

    datastores(datastoreId) = datastoreInfo
  }

  def receive = {
    // Worker
    case RegisterWorkerMessage(_workerInfo: WorkerInfo) =>
      val workerId = nextWorkerId
      nextWorkerId = incrementWorkerId(nextWorkerId)
      val datastoreId = nextTaskId
      nextTaskId += 1

      val workerInfo = _workerInfo.copy(
        workerId = workerId, mainDatastoreId = datastoreId)
      sender ! workerInfo

      totalCores += workerInfo.numCores

      val workerRef = AkkaUtil.getActorFromURL(
        workerInfo.workerAddress, context.system)
      log.info("Registering Worker at " + workerRef)

      context.watch(workerRef)
      scheduler.addWorker(workerId)

      workers(workerId) = workerRef
      workerInfos(workerId) = workerInfo

      launchMainDatastore(workerId)

      Stats.registeredWorkers += workerInfo

    case ScheduleTaskMessage
        (parent: ActorRef, _workerId: WorkerId, adjust: Adjustable[_]) =>
      val taskId = nextTaskId
      nextTaskId += 1

      val workerId =
        if (_workerId == -1) {
          scheduler.nextWorker()
        } else {
          _workerId
        }

      val workerRef = workers(workerId)

      val f = (workerRef ? CreateTaskMessage(taskId, parent))
        .mapTo[ActorRef]
      val respondTo = sender
      f.onComplete {
        case Success(taskRef) =>
          val taskInfo = new TaskInfo(
            taskId, taskRef, adjust, parent, workerId)
          tasks += taskInfo
          val outputFuture = taskRef ? RunTaskMessage(adjust)

          outputFuture.onComplete {
            case Success(output) =>
              respondTo ! (taskRef, output)
            case Failure(e) =>
              e.printStackTrace()
          }
        case Failure(e) =>
          e.printStackTrace()
      }

    // Mutator
    case RegisterMutatorMessage =>
      log.info("Registering mutator " + nextMutatorId)
      sender ! nextMutatorId
      nextMutatorId += 1

    case RunMutatorMessage(adjust: Adjustable[_], mutatorId: Int) =>
      log.info("Starting initial run for mutator " + mutatorId)

      val taskId = nextTaskId
      nextTaskId += 1

      val workerId = scheduler.nextWorker()
      val workerRef = workers(workerId)
      val taskRefFuture = workerRef ? CreateTaskMessage(taskId, workerRef)
      val taskRef = Await.result(taskRefFuture.mapTo[ActorRef], DURATION)

      val taskInfo = new TaskInfo(
        taskId, taskRef, adjust, workerRef, workerId)
      tasks += taskInfo

      (taskRef ? RunTaskMessage(adjust)) pipeTo sender

      rootTasks(mutatorId) = taskRef

    case PropagateMutatorMessage(mutatorId: Int) =>
      log.info("Initiating change propagation for mutator " + mutatorId)

      (rootTasks(mutatorId) ? PropagateTaskMessage) pipeTo sender

    case GetMutatorDDGMessage(mutatorId: Int) =>
      (rootTasks(mutatorId) ? GetTaskDDGMessage) pipeTo sender

    case PrintMutatorDDGDotsMessage(mutatorId, nextName, output) =>
      (rootTasks(mutatorId) ? PrintDDGDotsMessage(nextName, output)) pipeTo sender

    case ShutdownMutatorMessage(mutatorId: Int) =>
      log.info("Shutting down mutator " + mutatorId)

      datastores.clear()

      Await.result(Future.sequence(workers.map {
        case (workerId, workerRef) => workerRef ? ClearMessage
      }), DURATION)

      workers.map {
        case (workerId, workerRef) =>
          launchMainDatastore(workerId)
      }

      Stats.clear()
      if (rootTasks.contains(mutatorId)) {
        Await.result(rootTasks(mutatorId) ? ClearModsMessage, DURATION)

        val f = rootTasks(mutatorId) ? ShutdownTaskMessage
        rootTasks -= mutatorId

        Await.result(f, DURATION)
      }

      sender ! "done"

    // Datastore
    case CreateModMessage(value) =>
      (workers(scheduler.nextWorker()) ? CreateModMessage(value)) pipeTo sender

    case GetModMessage(modId: ModId, null) =>
      val datastoreRef = datastores(getDatastoreId(modId)).datastoreRef
      (datastoreRef ? GetModMessage(modId, null)) pipeTo sender

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      val datastoreRef = datastores(getDatastoreId(modId)).datastoreRef
      (datastoreRef ? PutMessage("mods", modId, value, null)) pipeTo sender

    case UpdateModMessage(modId: ModId, null, null) =>
      val datastoreRef = datastores(getDatastoreId(modId)).datastoreRef
      (datastoreRef ? PutMessage("mods", modId, null, null)) pipeTo sender

    case CreateListMessage(_conf: ListConf) =>
      val futures = Buffer[Future[(Map[TaskId, ActorRef], ObjHasher[ActorRef])]]()

      val inputId = nextInputId
      nextInputId += 1

      val conf =
        if (_conf.partitions == 0) {
          _conf.clone(partitions = totalCores, inputId = inputId)
        } else {
          _conf.clone(inputId = inputId)
        }

      var index = 0
      var hasher: ObjHasher[ActorRef] = null
      for ((workerId, workerRef) <- workers) {
        val thisHasher = createPartitions(workerId, workerRef, conf, index)

        if (hasher == null) {
          hasher = thisHasher
        } else {
          hasher = ObjHasher.combineHashers(hasher, thisHasher)
        }

        index += 1
      }

      assert(hasher.isComplete())

      val input = conf match {
        case aggregatorConf: AggregatorListConf =>
          new AggregatorInput(inputId, hasher, aggregatorConf, workers.values)

        case SimpleListConf(_, _, 1, _, false, _, _) =>
          new HashPartitionedDoubleListInput(
            inputId, hasher, conf, workers.values)

        case SimpleListConf(_, _, _, _, false, _, _) =>
          new HashPartitionedDoubleChunkListInput(
            inputId, hasher, conf, workers.values)
        case columnConf: ColumnListConf =>
          if (columnConf.chunkSize > 1)
            new ColumnChunkListInput(inputId, hasher, columnConf)
          else
            new ColumnListInput(inputId, hasher, columnConf)
        case _ => ???
      }

      sender ! input

    case FileLoadedMessage(datastoreId: TaskId, fileName: String) =>
      datastores(datastoreId).fileName = fileName
      sender ! "done"

    case Terminated(deadWorker: ActorRef) =>
      val newTasks = mutable.Set[TaskInfo]()
      log.warning("Received Terminated for " + deadWorker)

      val deadWorkerId = workers.find {
        case (workerId, workerRef) => workerRef == deadWorker
      }.get._1

      workers -= deadWorkerId
      workerInfos -= deadWorkerId

      scheduler.removeWorker(deadWorkerId)

      val futures = mutable.Buffer[Future[Any]]()
      datastores.map {
        case (id, info) =>
          if (info.workerId == deadWorkerId) {
            log.warning("Relaunching " + info)
            val workerRef = workers(scheduler.nextWorker())

            val modifierRef = Await.result(
              (workerRef ? CreateDatastoreMessage(
                info.listConf, info.id, info.range)).mapTo[ActorRef],
              DURATION)
            info.datastoreRef = modifierRef

            if (info.fileName != "") {
              futures += modifierRef ? LoadFileMessage(info.fileName, true)
            }
          }
      }

      Await.result(Future.sequence(futures), DURATION)

      Future {
      tasks.map {
        case info =>
          if (info.workerId == deadWorkerId) {
            log.warning("Relaunching " + info)
            val workerId = scheduler.nextWorker()
            val workerRef = workers(workerId)

            val f = (workerRef ? CreateTaskMessage(info.id, info.parent))
              .mapTo[ActorRef]

            f.onComplete {
              case Success(taskRef) =>
                val taskInfo = new TaskInfo(
                  info.id, taskRef, info.adjust, info.parent, workerId)
                newTasks += taskInfo
                val outputFuture = taskRef ? RunTaskMessage(info.adjust)
              case Failure(e) =>
                e.printStackTrace()
            }
          }
      }
      }

    case x =>
      log.warning("Master actor received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
