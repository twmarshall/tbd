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

  private val info = {
    val info = WorkerInfo(
      -1,
      systemURL + "/user/worker",
      systemURL + "/user/worker/datastore",
      webuiAddress,
      OS.getNumCores(),
      conf.storeType(),
      conf.envHomePath(),
      conf.cacheSize(),
      -1)
    val message = RegisterWorkerMessage(info)

    Await.result((masterRef ? message).mapTo[WorkerInfo], DURATION)
  }

  private val datastores = Map[TaskId, ActorRef]()

  private val datastore = context.actorOf(
    DatastoreActor.props(info, info.mainDatastoreId), "datastore")
  datastores(info.mainDatastoreId) = datastore

  def receive = {
    case PebbleMessage(taskRef: ActorRef, modId: ModId) =>
      sender ! "done"

    case CreateTaskMessage(taskId: TaskId, parent: ActorRef) =>
      val taskProps = Task.props(
        taskId, info.mainDatastoreId, parent, masterRef, datastores)
      val taskRef = context.actorOf(taskProps, taskId + "")

      sender ! taskRef

    case CreateDatastoreMessage
        (listConf: ListConf,
         datastoreId: TaskId,
         thisRange: HashRange) =>
      val modifierRef = listConf match {
        case aggregatorConf: AggregatorListConf =>
          context.actorOf(AggregatorModifierActor.props(
            aggregatorConf, info, datastoreId))
        case columnConf: ColumnListConf =>
          if (columnConf.chunkSize > 1)
            context.actorOf(ColumnChunkModifierActor.props(
              columnConf, info, datastoreId, thisRange))
          else
            context.actorOf(ColumnModifierActor.props(
              columnConf, info, datastoreId, thisRange))
        case _ =>
          context.actorOf(ModifierActor.props(
            listConf, info, datastoreId, thisRange))
      }
      datastores(datastoreId) = modifierRef
      sender ! modifierRef


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
