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
  def props(info: WorkerInfo, masterRef: ActorRef) =
    Props(classOf[Worker], info, masterRef)
}

class Worker(_info: WorkerInfo, masterRef: ActorRef)
    extends Actor with ActorLogging {
  import context.dispatcher

  log.info("Worker launched.")

  private val info = {
    val message = RegisterWorkerMessage(_info)

    Await.result((masterRef ? message).mapTo[WorkerInfo], DURATION)
  }
  private val datastores = Map[TaskId, ActorRef]()

  def receive = {
    case PebbleMessage(taskId: TaskId, modId: ModId) =>
      sender ! "done"

    case CreateTaskMessage(taskId: TaskId, parentId: TaskId) =>
      val taskProps = Task.props(
        taskId, info.mainDatastoreId, parentId, masterRef, datastores)
      val taskRef = context.actorOf(taskProps, taskId + "")

      sender ! taskRef

    case CreateDatastoreMessage(listConf, datastoreId: TaskId, thisRange, recovery) =>
      val modifierRef = listConf match {
        case null =>
          context.actorOf(DatastoreActor.props(info, datastoreId))
        case aggregatorConf: AggregatorListConf =>
          context.actorOf(AggregatorModifierActor.props(
            aggregatorConf, info, datastoreId, masterRef, recovery))
        case columnConf: ColumnListConf =>
          /*if (columnConf.chunkSize > 1)
            context.actorOf(ColumnChunkModifierActor.props(
              columnConf, info, datastoreId, thisRange))
          else*/
            context.actorOf(ColumnModifierActor.props(
              columnConf, info, datastoreId, thisRange))
        case _ =>
          context.actorOf(ModifierActor.props(
            listConf, info, datastoreId, thisRange, masterRef))
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

    case CreateModMessage(value) =>
      (datastores(info.mainDatastoreId) ? CreateModMessage(value)) pipeTo sender

    case ClearMessage =>
      for ((taskId, datastoreRef) <- datastores) {
        context.stop(datastoreRef)
      }
      datastores.clear()

      Stats.clear()

      sender ! "done"

    case "started" => sender ! "done"

    case x => println("Worker received unhandled message " + x)
  }
}
