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
  def props(conf: MasterConf): Props = Props(classOf[Master], conf)
}

class Master(conf: MasterConf) extends Actor with ActorLogging {
  import context.dispatcher

  Stats.registeredWorkers.clear()

  log.info("Master launched.")

  private val scheduler = new Scheduler()

  private val workers = Map[TaskId, ActorRef]()
  private val workerInfos = Map[TaskId, WorkerInfo]()

  private val tasks = mutable.Map[TaskId, TaskInfo]()

  private val datastores = Map[TaskId, DatastoreInfo]()

  // Maps mutatorIds to the Task the mutator's computation was launched on.
  private val rootTasks = Map[Int, ActorRef]()

  private val lists = Map[TaskId, ListInput[Any, Any]]()

  private var nextMutatorId = 0

  private var nextTaskId: TaskId = 1

  // The total number of cores reported by all registered Workers.
  private var totalCores = 0

  private var nextInputId: InputId = 0

  private def createPartitions
      (workerId: TaskId,
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
    var hasher: ObjHasher[TaskId] = null
    for (i <- 0 until partitions) {
      val start = workerIndex * partitionsPerWorker + i
      val thisRange = new HashRange(start, start + 1, listConf.partitions)
      val datastoreId = nextTaskId
      nextTaskId += 1

      val modifierRef = Await.result(
        (workerRef ? CreateDatastoreMessage(
          listConf, datastoreId, thisRange, false)).mapTo[ActorRef],
        DURATION)

      val thisHasher = ObjHasher.makeHasher(thisRange, datastoreId)
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

  private def launchMainDatastore(workerId: TaskId) {
    val datastoreId = workerInfos(workerId).mainDatastoreId
    val workerRef = workers(workerId)

    val message = CreateDatastoreMessage(null, datastoreId, null, false)

    val datastoreRef =
      Await.result((workerRef ? message).mapTo[ActorRef], DURATION)

    val datastoreInfo = new DatastoreInfo(
      datastoreId, datastoreRef, null, workerId, null)

    datastores(datastoreId) = datastoreInfo
  }

  def receive = {
    // Worker
    case RegisterWorkerMessage(_workerInfo: WorkerInfo) =>
      val workerId = nextTaskId
      nextTaskId += 1
      val datastoreId = nextTaskId
      nextTaskId += 1

      val workerInfo = _workerInfo.copy(
        workerId = workerId,
        mainDatastoreId = datastoreId,
        storeType = conf.storeType())
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
        (name: String, parentId: TaskId, datastoreId, adjust) =>
      val thisTask = tasks.find(_._2.name == name)
      if (name != "" && !thisTask.isEmpty) {
        val taskInfo = thisTask.get._2
        sender ! ((taskInfo.id, taskInfo.output))
      } else {
        val taskId = nextTaskId
        nextTaskId += 1

        val workerId =
          if (datastoreId == -1) {
            scheduler.nextWorker()
          } else {
            datastores(datastoreId).workerId
          }

        val workerRef = workers(workerId)

        val f = (workerRef ? CreateTaskMessage(taskId, parentId))
          .mapTo[ActorRef]
        val respondTo = sender
        f.onComplete {
          case Success(taskRef) =>
            val taskInfo = new TaskInfo(
              taskId, name, taskRef, adjust, parentId, workerId)
            tasks(taskId) = taskInfo
            val outputFuture = taskRef ? RunTaskMessage(adjust, false)

            outputFuture.onComplete {
              case Success(output) =>
                taskInfo.output = output
                respondTo ! (taskId, output)
              case Failure(e) =>
                e.printStackTrace()
            }
          case Failure(e) =>
            e.printStackTrace()
        }
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
      val taskRefFuture = workerRef ? CreateTaskMessage(taskId, workerId)
      val taskRef = Await.result(taskRefFuture.mapTo[ActorRef], DURATION)

      val taskInfo = new TaskInfo(
        taskId, "rootTask-" + mutatorId, taskRef, adjust, -1, workerId)
      tasks(taskId) = taskInfo

      val respondTo = sender
      (taskRef ? RunTaskMessage(adjust, false)).onComplete {
        case Success(output) =>
          taskInfo.output = output
          respondTo ! output
        case Failure(e) => e.printStackTrace()
      }

      rootTasks(mutatorId) = taskRef

    case PropagateMutatorMessage(mutatorId: Int) =>
      log.info("Initiating change propagation for mutator " + mutatorId)

      (rootTasks(mutatorId) ? PropagateTaskMessage) pipeTo sender

    case GetMutatorDDGMessage(mutatorId: Int) =>
      (rootTasks(mutatorId) ? GetTaskDDGMessage) pipeTo sender

    case PrintMutatorDDGDotsMessage(mutatorId, nextName, output) =>
      (rootTasks(mutatorId) ? PrintDDGDotsMessage(nextName, output)) pipeTo sender

    case MutatorToBufferMessage(datastoreId) =>
      (datastores(datastoreId).datastoreRef ? ToBufferMessage()) pipeTo sender

    case ShutdownMutatorMessage(mutatorId: Int) =>
      log.info("Shutting down mutator " + mutatorId)

      datastores.clear()

      Await.result(Future.sequence(workers.map {
        case (workerId, workerRef) =>
          workerRef ? ClearMessage
      }), DURATION)

      workers.map {
        case (workerId, workerRef) =>
          launchMainDatastore(workerId)
      }

      Stats.clear()

      tasks.map {
        case (taskId, taskInfo) =>
          context.stop(taskInfo.taskRef)
      }
      tasks.clear()
      rootTasks.clear()

      sender ! "done"

    case ResolveMessage(taskId) =>
      if (datastores.contains(taskId)) {
        sender ! datastores(taskId).datastoreRef
      } else if (workers.contains(taskId)) {
        sender ! workers(taskId)
      } else {
        val option = tasks.find {
          case (id, taskInfo) =>
            taskInfo.id == taskId
        }
        assert(!option.isEmpty)
        sender ! option.get._2.taskRef
      }

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

    case CreateListMessage(_conf: ListConf, taskId: TaskId) =>
      if (taskId != -1 && tasks(taskId).recovering) {
        sender ! lists(taskId)
      } else {
        val inputId = nextInputId
        nextInputId += 1

        val conf =
          if (_conf.partitions == 0) {
            _conf.clone(partitions = totalCores, inputId = inputId)
          } else {
            _conf.clone(inputId = inputId)
          }

        var index = 0
        var hasher: ObjHasher[TaskId] = null
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
            new AggregatorInput(inputId, hasher, aggregatorConf, workers.values, self)

          case SimpleListConf(_, _, 1, _, false, _, _) =>
            new HashPartitionedDoubleListInput(
              inputId, hasher, conf, workers.values, self)

          case SimpleListConf(_, _, _, _, false, _, _) =>
            new HashPartitionedDoubleChunkListInput(
              inputId, hasher, conf, workers.values, self)
          case columnConf: ColumnListConf =>
            if (columnConf.chunkSize > 1)
              new ColumnChunkListInput(inputId, hasher, columnConf, self)
            else
              new ColumnListInput(inputId, hasher, columnConf, self)
          case _ => ???
        }

        lists(taskId) = input.asInstanceOf[ListInput[Any, Any]]
        sender ! input
      }

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

      tasks.map {
        case (id, info) =>
          if (info.workerId == deadWorkerId) {
            info.recovering = true
          }
      }

      // Relaunch datastores.
      val futures = mutable.Buffer[Future[Any]]()
      datastores.map {
        case (id, info) =>
          if (info.workerId == deadWorkerId) {
            log.warning("Relaunching " + info)
            val workerId = scheduler.nextWorker()
            val workerRef = workers(workerId)
            context.stop(info.datastoreRef)
            val modifierRef = Await.result(
              (workerRef ? CreateDatastoreMessage(
                info.listConf, info.id, info.range, true)).mapTo[ActorRef],
              DURATION)
            info.datastoreRef = modifierRef
            info.workerId = workerId

            if (info.fileName != "") {
              futures += modifierRef ? LoadFileMessage(info.fileName, true)
            }
          }
      }

      Await.result(Future.sequence(futures), DURATION)

      // Relaunch tasks.
      Future {
        val futures = tasks.flatMap {
          case (id, info) =>
            if (info.workerId == deadWorkerId) {
              log.warning("Relaunching " + info)
              val workerId = scheduler.nextWorker()
              val workerRef = workers(workerId)
              context.stop(info.taskRef)
              Iterable((info, (workerRef ? CreateTaskMessage(info.id, info.parentId))
                .mapTo[ActorRef]))
            } else {
              Iterable()
            }
        }

        val runFutures = futures.map {
          case (info, future) =>
            val taskRef = Await.result(future, DURATION)
            info.taskRef = taskRef
            info.workerId = info.workerId

            if (info.name.startsWith("rootTask")) {
              val mutatorId = info.name.substring(
                info.name.indexOf("-")).toInt
              rootTasks(mutatorId) = taskRef
            }

            taskRef ? RunTaskMessage(info.adjust, true)
        }

        Await.result(Future.sequence(runFutures), DURATION)
        tasks.map {
          case (id, info) => info.recovering = false
        }
      }

    case x =>
      log.warning("Master actor received unhandled message " +
                  x + " from " + sender + " " + x.getClass)
  }
}
