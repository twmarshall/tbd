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
package tbd.datastore

import akka.actor.{Actor, ActorRef, ActorLogging, Props}
import akka.pattern.ask
import akka.util.Timeout
import scala.collection.mutable.{Map, Set}
import scala.concurrent.Await
import scala.concurrent.duration._

import tbd.ListNode
import tbd.messages._
import tbd.mod.{InputMod, Mod, ModId}

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  private val tables = Map[String, Map[Any, Any]]()
  tables("mods") = Map[Any, Any]()

  // Maps modIds to the workers that have read the corresponding mod.
  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps the name of an input table to a set containing the datasets that were
  // returned containing this entire table, so that we can tolerate insertions
  // into and deletions from tables.
  private val datasets = Map[String, Set[Dataset[Any]]]()

  implicit val timeout = Timeout(30 seconds)

  private def get(table: String, key: Any): Any = {
    if (!tables(table).contains(key)) {
      log.warning("Trying to get nonexistent key = " + key + " from table = " +
                  table)
    }
    val ret = tables(table)(key)

    if (ret == null) {
      NullMessage
    } else {
      ret
    }
  }

  private def createMod[T](value: T): Mod[T] = {
    val mod = new InputMod[T](self)
    tables("mods")(mod.id.value) = value
    dependencies(mod.id) = Set[ActorRef]()
    mod
  }

  private def updateMod(modId: ModId, value: Any, sender: ActorRef): Int = {
    if (!tables("mods").contains(modId.value)) {
      log.warning("Trying to update non-existent mod.")
    }
    tables("mods")(modId.value) = value

    var count = 0
    for (workerRef <- dependencies(modId)) {
      if (workerRef != sender) {
        workerRef ! ModUpdatedMessage(modId, sender)
        count += 1
      }
    }

    count
  }

  private def remove(
      toRemove: Mod[Any], dataset: Dataset[Any], respondTo: ActorRef): Int = {
    var count = 0
    var outerNode = tables("mods")(dataset.lists.id.value)
      .asInstanceOf[ListNode[Mod[ListNode[Any]]]]
    var found = false
    while (outerNode != null && !found) {
      val modList = tables("mods")(outerNode.value.id.value)
        .asInstanceOf[Mod[ListNode[Any]]]
      var innerNode = tables("mods")(modList.id.value)
        .asInstanceOf[ListNode[Any]]

      var previousNode: ListNode[Any] = null
      while (innerNode != null && !found) {
        if (innerNode.value == toRemove) {
          if (previousNode != null) {
            count += updateMod(previousNode.next.id,
                               tables("mods")(innerNode.next.id.value),
                               respondTo)
          } else {
            count += updateMod(modList.id,
                               tables("mods")(innerNode.next.id.value),
                               respondTo)
          }

          tables("mods") -= innerNode.next.id.value
	  tables("mods") -= innerNode.value.id.value

	  found = true
        } else {
          previousNode = innerNode
          innerNode = tables("mods")(innerNode.next.id.value)
            .asInstanceOf[ListNode[Any]]
        }
      }
      outerNode = tables("mods")(outerNode.next.id.value)
        .asInstanceOf[ListNode[Mod[ListNode[Any]]]]
    }

    if (!found) {
      log.warning("Didn't find value to remove.")
    }

    count
  }

  private def insert(
      mod: Mod[Any],
      dataset: Dataset[Any],
      respondTo: ActorRef): Int = {
    var count = 0
    var outerNode = tables("mods")(dataset.lists.id.value)
      .asInstanceOf[ListNode[Mod[ListNode[Any]]]]

    var lastMod: Mod[ListNode[Any]] = null
    while (outerNode != null) {
      val modList = tables("mods")(outerNode.value.id.value)
        .asInstanceOf[Mod[ListNode[Any]]]
      var innerNode = tables("mods")(modList.id.value)
        .asInstanceOf[ListNode[Any]]

      while (innerNode != null) {
        lastMod = innerNode.next
        innerNode = tables("mods")(innerNode.next.id.value)
          .asInstanceOf[ListNode[Any]]
      }
      outerNode = tables("mods")(outerNode.next.id.value)
        .asInstanceOf[ListNode[Mod[ListNode[Any]]]]
    }

    val newTail = createMod[ListNode[Any]](null)
    updateMod(lastMod.id, new ListNode(mod, newTail), respondTo)
  }

  def receive = {
    case CreateTableMessage(table: String) => {
      tables(table) = Map[Any, Any]()
    }

    case GetMessage(table: String, key: Any) => {
      sender ! get(table, key)
    }

    case PutMessage(table: String, key: Any, value: Any, respondTo: ActorRef) => {
      if (tables(table).contains(key)) {
	log.warning("Putting input key that already exists.")
      }

      val mod = createMod(value)
      tables(table)(key) = mod

      var count = 0
      if (datasets.contains(table)) {
        for (dataset <- datasets(table)) {
          count += insert(mod, dataset, respondTo)
        }
      }

      sender ! count
    }

    case UpdateMessage(table: String, key: Any, value: Any, respondTo: ActorRef) => {
      //log.debug("UpdateMessage")
      val modId = tables(table)(key).asInstanceOf[Mod[Any]].id
      sender ! updateMod(modId, value, respondTo)
    }

    case RemoveMessage(table: String, key: Any, respondTo: ActorRef) => {
      //log.debug("RemoveMessage")

      var count = 0
      if (datasets.contains(table)) {
        for (dataset <- datasets(table)) {
	  if (!tables(table).contains(key)) {
	    log.warning("Trying to remove non-existent key from table.")
	  }
          val mod = tables(table)(key).asInstanceOf[Mod[Any]]

	  if (!tables("mods").contains(mod.id.value)) {
	    log.warning("Trying to remove non-existent mod " + mod.id)
	  }
          count += remove(tables(table)(key).asInstanceOf[Mod[Any]], dataset, respondTo)
        }
      }

      tables(table) -= key

      sender ! count
    }

    case UpdateModMessage(modId: ModId, value: Any, workerRef: ActorRef) => {
      sender ! updateMod(modId, value, workerRef)
    }

    case UpdateModMessage(modId: ModId, null, workerRef: ActorRef) => {
      sender ! updateMod(modId, null, workerRef)
    }

    case ReadModMessage(modId: ModId, workerRef: ActorRef) => {
      dependencies(modId) += workerRef
      sender ! get("mods", modId.value)
    }

    case CleanUpMessage(workerRef: ActorRef, removeDatasets: Set[Dataset[Any]]) => {
      //log.debug("RemoveDependenciesMessage from " + sender)

      for ((modId, dependencySet) <- dependencies) {
        dependencySet -= workerRef
      }

      for ((table, sets) <- datasets) {
        for (removeDataset <- removeDatasets) {
          sets -= removeDataset
        }
      }

      sender ! "done"
    }

    // Return all of the entries from the specified table as an array, allowing
    // for updates to existing elements to be propagated.
    case GetArrayMessage(table: String) => {
      val arr = new Array[Mod[Any]](tables(table).size)

      var i = 0
      for (elem <- tables(table)) {
        arr(i) = elem._2.asInstanceOf[Mod[Any]]
        i += 1
      }

      sender ! arr
    }

    case GetDatasetMessage(table: String, partitions: Int) => {
      var tail = createMod[ListNode[Any]](null)

      var outputTail = createMod[ListNode[Mod[ListNode[Any]]]](null)
      val partitionSize = math.max(1, tables(table).size / partitions)
      var i = 1
      for (elem <- tables(table)) {
        val head = createMod(new ListNode(elem._2.asInstanceOf[Mod[Any]], tail))
        if (i % partitionSize == 0) {
          val headMod = createMod(head)
          outputTail = createMod(new ListNode[Mod[ListNode[Any]]](headMod, outputTail))
          tail = createMod[ListNode[Any]](null)
        } else {
          tail = head
        }
        i += 1
      }
      if ((i - 1) % partitionSize != 0) {
        val headMod = createMod(tail)
        outputTail = createMod(new ListNode(headMod, outputTail))
      }

      val dataset = new Dataset(outputTail)

      if (datasets.contains(table)) {
        datasets(table) += dataset
      } else {
        datasets(table) = Set(dataset)
      }

      sender ! dataset
    }

    case x => {
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
