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
import tbd.mod.{Matrix, Mod, ModId}

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  private val tables = Map[String, Map[Any, Any]]()
  tables("mods") = Map[Any, Any]()
  tables("memo") = Map[Any, Any]()

  private val dependencies = Map[ModId, Set[ActorRef]]()

  // Maps the name of an input table to a set containing mods representing the
  // tails of any linked lists that were returned containing this entire table,
  // so that we can tolerate unsertions into tables.
  private val lists = Map[String, Set[Mod[ListNode[Any]]]]()

  private var updated = Set[ModId]()

  implicit val timeout = Timeout(5 seconds)

  private def get(table: String, key: Any): Any = {
    val ret = tables(table)(key)

    if (ret == null) {
      NullMessage
    } else {
      ret
    }
  }

  private def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T](self)
    tables("mods")(mod.id.value) = value
    dependencies(mod.id) = Set[ActorRef]()
    mod
  }

  private def updateMod(modId: ModId, value: Any, sender: ActorRef): Boolean = {
    log.debug("Updating mod(" + modId+ ")")
    tables("mods")(modId.value) = value

    for (workerRef <- dependencies(modId)) {
      if (workerRef != sender) {
        log.debug("Informing " + workerRef + " that mod(" +
                  modId + ") has been updated.")
        val future = workerRef ? ModUpdatedMessage(modId)
        Await.result(future, timeout.duration)
      }
    }

    updated += modId

    true
  }

  def receive = {
    case CreateTableMessage(table: String) => {
      tables(table) = Map[Any, Any]()
    }

    case GetMessage(table: String, key: Any) => {
      sender ! get(table, key)
    }

    case PutMessage(table: String, key: Any, value: Any) => {
      log.debug("PutMessage")
      val mod = createMod(value)
      tables(table)(key) = mod

      if (lists.contains(table)) {
        val newTails = Set[Mod[ListNode[Any]]]()
        for (tail <- lists(table)) {
          log.debug("Appending to list for " + table)
          val newTail = createMod[ListNode[Any]](null)
          updateMod(tail.id, new ListNode(mod, newTail), sender)
          newTails += newTail
        }
        lists(table) = newTails
      }

      sender ! mod.id
    }

    case UpdateMessage(table: String, key: Any, value: Any) => {
      log.debug("UpdateMessage(" + table + ", " + key + ", " + value +")")
      val modId = tables(table)(key).asInstanceOf[Mod[Any]].id
      updateMod(modId, value, sender)
      sender ! modId
    }

    case CreateModMessage(value: Any) => {
      sender ! createMod(value)
    }

    case CreateModMessage(null) => {
      sender ! createMod(null)
    }

    case UpdateModMessage(modId: ModId, value: Any) => {
      sender ! updateMod(modId, value, sender)
    }

    case UpdateModMessage(modId: ModId, null) => {
      sender ! updateMod(modId, null, sender)
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

    // Return all of the entries from the specified table as a linked list,
    // allowing for both updates to existing elements and insertions to be
    // propagated.
    case GetListMessage(table: String) => {
      var tail = createMod[ListNode[Any]](null)

      if (lists.contains(table)) {
        lists(table) += tail
      } else {
        lists(table) = Set(tail)
      }

      for (elem <- tables(table)) {
        val head = createMod(new ListNode(elem._2.asInstanceOf[Mod[Any]], tail))
        tail = head
      }

      sender ! tail
    }

    case GetUpdatedMessage => {
      sender ! updated
    }

    case x => {
      log.warning("Datastore actor received unhandled message " +
                  x + " from " + sender)
    }
  }
}
