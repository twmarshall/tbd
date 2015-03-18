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

  private var nextDatastoreId: DatastoreId = 1

  private val datastore = context.actorOf(
    DatastoreActor.props(workerInfo), "datastore")

  private val datastores = Map[DatastoreId, ActorRef]()
  datastores(0) = datastore

  // A unique id to assign to tasks forked from this context. The datastore
  // has TaskId = 0 for the purpose of creating ModIds.
  private var nextTaskId: TaskId = 1

  var nextListId = 0

  def receive = {
    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      sender ! "done"

    case CreateTaskMessage(parent: ActorRef) =>
      val taskProps = Task.props(nextTaskId, workerInfo.workerId, parent, masterRef, datastores)
      val taskRef = context.actorOf(taskProps, nextTaskId + "")

      sender ! taskRef

      nextTaskId += 1


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

        val modifierRef = listConf match {
          case aggregatorConf: AggregatorListConf[Any] =>
            context.actorOf(AggregatorModifierActor.props(
              aggregatorConf, workerInfo, nextDatastoreId))
          case _ =>
            context.actorOf(ModifierActor.props(
              listConf, workerInfo, nextDatastoreId, thisRange))
        }

        val thisHasher = ObjHasher.makeHasher(thisRange, modifierRef)
        if (hasher == null) {
          hasher = thisHasher
        } else {
          hasher = ObjHasher.combineHashers(thisHasher, hasher)
        }

        newDatastores(nextDatastoreId) = modifierRef
        nextDatastoreId = (nextDatastoreId + 1).toShort
      }
      datastores ++= newDatastores

      sender ! (newDatastores, hasher)

    case SplitFileMessage(dir: String, fileName: String, partitions: Int) =>
      if (!OS.exists(dir)) {
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
