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
import scala.collection.mutable
import scala.collection.mutable.Map
import scala.concurrent.{Await, Promise}
import scala.util.{Failure, Success}

import tdb.Adjustable
import tdb.Constants._
import tdb.datastore._
import tdb.list._
import tdb.messages._
import tdb.stats.Stats
import tdb.util._

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

  private val workerInfo = {
    val workerInfo = WorkerInfo(
      -1,
      systemURL + "/user/worker",
      systemURL + "/user/worker/datastore",
      webuiAddress,
      OS.getNumCores(),
      conf.storeType(),
      conf.envHomePath(),
      conf.cacheSize())
    val message = RegisterWorkerMessage(workerInfo)

    Await.result((masterRef ? message).mapTo[WorkerInfo], DURATION)
  }

  private val datastores = Map[DatastoreId, ActorRef]()

  private var nextDatastoreId: DatastoreId = 1
  private val datastoreId =
    createDatastoreId(workerInfo.workerId, nextDatastoreId)
  nextDatastoreId = (nextDatastoreId + 1).toShort
  private val datastore = context.actorOf(
    DatastoreActor.props(workerInfo, datastoreId), "datastore")
  datastores(datastoreId) = datastore

  def receive = {
    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      sender ! "done"

    case CreateTaskMessage(taskId: TaskId, parent: ActorRef) =>
      val taskProps = Task.props(
        taskId, workerInfo.workerId, parent, masterRef, datastores)
      val taskRef = context.actorOf(taskProps, taskId + "")

      sender ! taskRef

    case CreateListIdsMessage
        (listConf: ListConf, workerIndex: Int, numWorkers: Int) =>
      log.debug("CreateListIdsMessage " + listConf)

      val partitionsPerWorker = listConf.partitions / numWorkers
      val partitions =
        if (workerIndex == numWorkers - 1 &&
            listConf.partitions % numWorkers != 0) {
          numWorkers % partitionsPerWorker
        } else {
          partitionsPerWorker
        }

      val newDatastores = Map[DatastoreId, ActorRef]()
      var hasher: ObjHasher[ActorRef] = null
      for (i <- 0 until partitions) {
        val start = workerIndex * partitionsPerWorker + i
        val thisRange = new HashRange(start, start + 1, listConf.partitions)
        val newDatastoreId = createDatastoreId(
          workerInfo.workerId, nextDatastoreId)
        nextDatastoreId = (nextDatastoreId + 1).toShort

        val modifierRef = listConf match {
          case aggregatorConf: AggregatorListConf =>
            context.actorOf(AggregatorModifierActor.props(
              aggregatorConf, workerInfo, newDatastoreId))
          case columnConf: ColumnListConf =>
            if (columnConf.chunkSize > 1)
              context.actorOf(ColumnChunkModifierActor.props(
                columnConf, workerInfo, newDatastoreId, thisRange))
            else
              context.actorOf(ColumnModifierActor.props(
                columnConf, workerInfo, newDatastoreId, thisRange))
          case _ =>
            context.actorOf(ModifierActor.props(
              listConf, workerInfo, newDatastoreId, thisRange))
        }

        val thisHasher = ObjHasher.makeHasher(thisRange, modifierRef)
        if (hasher == null) {
          hasher = thisHasher
        } else {
          hasher = ObjHasher.combineHashers(thisHasher, hasher)
        }

        newDatastores(newDatastoreId) = modifierRef
      }
      datastores ++= newDatastores

      sender ! (newDatastores, hasher)

    case SplitFileMessage(dir: String, fileName: String, partitions: Int) =>
      if (tdb.examples.Experiment.fast) {
        if (!OS.exists(dir)) {
          OS.mkdir(dir)

          tdb.scripts.Split.main(Array(
            "-d", dir,
            "-f", fileName,
            "-p", partitions + ""))
        }
      } else {
        if (OS.exists(dir)) {
          OS.rmdir(dir)
        }

        OS.mkdir(dir)

        tdb.scripts.Split.main(Array(
          "-d", dir,
          "-f", fileName,
          "-p", partitions + ""))
      }

      sender ! "done"

    case CreateModMessage(value: Any) =>
      (datastore ? CreateModMessage(value)) pipeTo sender

    case CreateModMessage(null) =>
      (datastore ? CreateModMessage(null)) pipeTo sender

    case ClearMessage =>
      Stats.clear()

    case "started" => sender ! "done"

    case x => println("Worker received unhandled message " + x)
  }
}
