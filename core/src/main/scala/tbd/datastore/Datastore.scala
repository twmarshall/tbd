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
import tbd.mod.{InputMod, Matrix, Mod, ModId}

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  private val tables = Map[String, Map[Any, Any]]()
  tables("mods") = Map[Any, Any]()
  tables("memo") = Map[Any, Any]()

  // Maps modIds to the workers that have read the corresponding mod.
  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps the name of an input table to a set containing mods representing the
  // tails of any linked lists that were returned containing this entire table,
  // so that we can tolerate insertions into tables.
  private val lists = Map[String, Set[Mod[ListNode[Any]]]]()

  implicit val timeout = Timeout(30 seconds)

  private def get(table: String, key: Any): Any = {
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

  private def getLists(table: String): Mod[ListNode[Mod[ListNode[Any]]]] = {
    var tail = createMod[ListNode[Any]](null)

    if (lists.contains(table)) {
      lists(table) += tail
    } else {
      lists(table) = Set(tail)
    }

    var outputTail = createMod[ListNode[Mod[ListNode[Any]]]](null)
    val partitionSize = 100
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

    outputTail
  }

  def receive = {
    case CreateTableMessage(table: String) => {
      tables(table) = Map[Any, Any]()
    }

    case GetMessage(table: String, key: Any) => {
      sender ! get(table, key)
    }

    case PutMessage(table: String, key: Any, value: Any, respondTo: ActorRef) => {
      val mod = createMod(value)
      tables(table)(key) = mod

      var count = 0
      if (lists.contains(table)) {
        val newTails = Set[Mod[ListNode[Any]]]()
        for (tail <- lists(table)) {
          val newTail = createMod[ListNode[Any]](null)
          count += updateMod(tail.id, new ListNode(mod, newTail), respondTo)
          newTails += newTail
        }
        lists(table) = newTails
      }

      sender ! count
    }

    case UpdateMessage(table: String, key: Any, value: Any, respondTo: ActorRef) => {
      val modId = tables(table)(key).asInstanceOf[Mod[Any]].id
      sender ! updateMod(modId, value, respondTo)
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

    case PutMatrixMessage(table: String, key: Any, value: Array[Array[Int]]) => {
      val mat = new Matrix(value.map(row => {
        row.map(cell => {
          createMod(cell)
        })
      }), self)
      tables(table)(key) = mat
      sender ! mat
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

    case GetDatasetMessage(table: String) => {
      sender ! new Dataset(getLists(table))
    }

    case x => {
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
