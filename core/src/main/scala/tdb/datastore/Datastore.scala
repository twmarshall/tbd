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
package tdb.datastore

import akka.actor.{Actor, ActorLogging, ActorRef, Props}
import akka.pattern.ask
import java.io.File
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}

import tdb.Mod
import tdb.Constants._
import tdb.list._
import tdb.messages._
import tdb.stats.Stats
import tdb.util.FileUtil

object Datastore {
  def props(storeType: String, cacheSize: Int): Props =
    Props(classOf[Datastore], storeType, cacheSize)
}

class Datastore
    (storeType: String,
     cacheSize: Int) extends Actor with ActorLogging {
  import context.dispatcher

  var workerId: WorkerId = _

  private val store = storeType match {
    case "memory" => new MemoryStore()
    case "berkeleydb" => new BerkeleyDBStore(cacheSize, context)
  }

  private var nextModId = 0

  private val lists = Map[String, ListInput[Any, Any]]()

  private var nextListId = 0

  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps logical names of datastores to their references.
  private val datastores = Map[WorkerId, ActorRef]()

  private var misses = 0

  def createMod[T](value: T): Mod[T] = {
    var newModId: Long = workerId
    newModId = newModId << 48
    newModId += nextModId

    nextModId += 1

    store.put(newModId, value)

    new Mod(newModId)
  }

  def read[T](mod: Mod[T]): T = {
    store.get(mod.id).asInstanceOf[T]
  }

  def getMod(modId: ModId, taskRef: ActorRef): Any = {
    if (store.contains(modId)) {
      val value = store.get(modId)
      if (value == null)
        NullMessage
      else
        value
    } else {
      val workerId = getWorkerId(modId)
      val future = datastores(workerId) ? GetModMessage(modId, taskRef)

      Stats.datastoreMisses += 1

      val result = Await.result(future, DURATION)

      if (result.isInstanceOf[Tuple2[_, _]]) {
        result.toString
      }

      result
    }
  }

  def update[T](mod: Mod[T], value: T) {
    val futures = Buffer[Future[String]]()

    if (!store.contains(mod.id) || store.get(mod.id) != value) {
      store.put(mod.id, value)

      if (dependencies.contains(mod.id)) {
        for (taskRef <- dependencies(mod.id)) {
          futures += (taskRef ? ModUpdatedMessage(mod.id)).mapTo[String]
        }
      }
    }

    Await.result(Future.sequence(futures), DURATION)
  }

  def asyncUpdate[T](mod: Mod[T], value: T): Future[_] = {
    val futures = Buffer[Future[String]]()

    if (!store.contains(mod.id) || store.get(mod.id) != value) {
      store.put(mod.id, value)

      if (dependencies.contains(mod.id)) {
        for (taskRef <- dependencies(mod.id)) {
          futures += (taskRef ? ModUpdatedMessage(mod.id)).mapTo[String]
        }
      }
    }

    Future.sequence(futures)
  }

  def updateMod
      (modId: ModId,
       value: Any,
       task: ActorRef,
       respondTo: ActorRef) {
    val futures = Buffer[Future[String]]()

    if (!store.contains(modId) || store.get(modId) != value) {
      store.put(modId, value)

      if (dependencies.contains(modId)) {
        for (taskRef <- dependencies(modId)) {
          if (taskRef != task) {
            futures += (taskRef ? ModUpdatedMessage(modId)).mapTo[String]
          }
        }
      }
    }

    Future.sequence(futures).onComplete {
      case value => respondTo ! "done"
    }
  }

  def receive = {
    case CreateModMessage(value: Any) =>
      sender ! createMod(value)

    case CreateModMessage(null) =>
      sender ! createMod(null)

    case GetModMessage(modId: ModId, taskRef: ActorRef) =>
      sender ! getMod(modId, taskRef)

      if (dependencies.contains(modId)) {
        dependencies(modId) += taskRef
      } else {
        dependencies(modId) = Set(taskRef)
      }

    case GetModMessage(modId: ModId, null) =>
      sender ! getMod(modId, null)

    case UpdateModMessage(modId: ModId, value: Any, task: ActorRef) =>
      updateMod(modId, value, task, sender)

    case UpdateModMessage(modId: ModId, value: Any, null) =>
      updateMod(modId, value, null, sender)

    case UpdateModMessage(modId: ModId, null, task: ActorRef) =>
      updateMod(modId, null, task, sender)

    case UpdateModMessage(modId: ModId, null, null) =>
      updateMod(modId, null, null, sender)

    case RemoveModsMessage(modIds: Iterable[ModId]) =>
      for (modId <- modIds) {
        store.remove(modId)
        dependencies -= modId
      }

      sender ! "done"

    case CreateListIdsMessage(conf: ListConf, numPartitions: Int) =>
      val listIds = Buffer[String]()

      val newLists = Buffer[ListInput[Any, Any]]()
      for (i <- 1 to numPartitions) {
        val listId = nextListId + ""
        nextListId += 1
        val list =
          if (conf.double) {
            if (conf.chunkSize == 1)
              new DoubleListModifier[Any, Any](this)
            else
              new DoubleChunkListModifier[Any, Any](this, conf)
          } else if (conf.chunkSize == 1) {
            new ListModifier[Any, Any](this)
          } else {
            new ChunkListModifier[Any, Any](this, conf)
          }

        lists(listId) = list
        newLists += list
        listIds += listId
      }

      sender ! listIds

    case LoadPartitionsMessage
        (fileName: String,
         partitions: Iterable[Partition[String, String]],
         numWorkers: Int,
         workerIndex: Int) =>

      val theseLists = partitions.map {
        case partition: Partition[String, String] =>
          lists(partition.partitionId)
      }.toBuffer

      val file = new File(fileName)
      val fileSize = file.length()
      val partitionSize = fileSize / numWorkers

      var nextList = 0
      val process = (key: String, value: String) => {
        theseLists(nextList).put(key, value)
        nextList = (nextList + 1) % theseLists.size
      }

      val readFrom = partitionSize * workerIndex
      val readSize =
        if (workerIndex == numWorkers - 1) {
          // If the file doesn't divide by the number of partitions evenly,
          // we process the extra along with the last partition.
          fileSize - partitionSize * (numWorkers - 1)
        } else {
          partitionSize
        }

      log.debug("Reading " + fileName + " from " + readFrom + ", size " +
                readSize)

      FileUtil.readKeyValueFile(fileName, fileSize, readFrom, readSize, process)

      sender ! "done"

    case GetAdjustableListMessage(listId: String) =>
      sender ! lists(listId).getAdjustableList()

    case LoadMessage(listId: String, data: Map[Any, Any]) =>
      lists(listId).load(data)
      sender ! "okay"

    case PutMessage(listId: String, key: Any, value: Any) =>
      // This solves a bug where sometimes deserialized Scala objects show up as
      // null in matches. We should figure out a better way of solving this.
      key.toString
      value.toString

      lists(listId).put(key, value)
      sender ! "okay"

    case UpdateMessage(listId: String, key: Any, value: Any) =>
      lists(listId).update(key, value)
      sender ! "okay"

    case RemoveMessage(listId: String, key: Any, value: Any) =>
      lists(listId).remove(key, value)
      sender ! "okay"

    case PutAfterMessage(listId: String, key: Any, newPair: (Any, Any)) =>
      lists(listId).putAfter(key, newPair)
      sender ! "okay"

    case RegisterDatastoreMessage(workerId: WorkerId, datastoreRef: ActorRef) =>
      datastores(workerId) = datastoreRef

    case SetIdMessage(_workerId: WorkerId) =>
      workerId = _workerId

    case ClearMessage() =>
      store.clear()
      lists.clear()

    case x =>
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
  }
}