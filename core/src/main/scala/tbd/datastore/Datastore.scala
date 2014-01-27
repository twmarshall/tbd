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
import scala.collection.mutable.Map

import tbd.ListNode
import tbd.messages._
import tbd.mod.{Matrix, Mod}

object Datastore {
  def props(): Props = Props(classOf[Datastore])
}

class Datastore extends Actor with ActorLogging {
  private val tables = Map[String, Map[Int, Any]]()
  tables("mods") = Map[Int, Any]()

  private var updated = Set[Int]()

  private def createTable(table: String) {
    tables(table) = Map[Int, Any]()
  }

  private def get(table: String, key: Int): Any = {
    val ret = tables(table)(key)
    if (ret == null) {
      NullMessage
    } else {
      ret
    }
  }

  private def put(table: String, key: Int, value: Any) {
    tables(table)(key) = value
  }

  private def putMod(table: String, key: Int, value: Any): Mod[Any] = {
    val mod = createMod(value)
    tables(table)(key) = mod
    mod
  }

  private def createMod[T](value: T): Mod[T] = {
    val mod = new Mod[T](self)
    tables("mods")(mod.id.value) = value
    mod
  }

  private def updateMod(key: Int, value: Any) {
    tables("mods")(key) = value
    updated += key
  }

  private def putMatrix(table: String, key: Int, value: Array[Array[Int]]): Matrix = {
    val mat = new Matrix(value.map(row => {
      row.map(cell => {
        createMod(cell)
      })
    }), self)
    tables(table)(key) = mat
    mat
  }

  private def asArray(table: String): Array[Mod[Any]] = {
    val arr = new Array[Mod[Any]](tables(table).size)

    var i = 0
    for (elem <- tables(table)) {
      arr(i) = elem._2.asInstanceOf[Mod[Any]]
      i += 1
    }

    arr
  }

  private def asList(table: String): Mod[ListNode[Any]] = {
    var tail = new Mod[ListNode[Any]](self)
    tables("mods")(tail.id.value) = null

    for (elem <- tables(table)) {
      val head = new Mod[ListNode[Any]](self)
      tables("mods")(head.id.value) = new ListNode(elem._2.asInstanceOf[Mod[Any]], tail)
      tail = head
    }

    tail
  }

  private def getUpdated(): Set[Int] =
    updated

  def receive = {
    case CreateTableMessage(table: String) =>
      createTable(table)
    case GetMessage(table: String, key: Int) =>
      sender ! get(table, key)
    case PutMessage(table: String, key: Int, value: Any) =>
      put(table, key, value)
    case PutModMessage(table: String, key: Int, value: Any) =>
      sender ! putMod(table, key, value)
    case CreateModMessage(value: Any) =>
      sender ! createMod(value)
    case CreateModMessage(null) =>
      sender ! createMod(null)
    case UpdateModMessage(key: Int, value: Any) =>
      updateMod(key, value)
    case PutMatrixMessage(table: String, key: Int, value: Array[Array[Int]]) =>
      sender ! putMatrix(table, key, value)
    case GetArrayMessage(table: String) =>
      sender ! asArray(table)
    case GetListMessage(table: String) =>
      sender ! asList(table)
    case GetUpdatedMessage =>
      sender ! getUpdated()
    case x => log.warning("Datastore actor received unhandled message " +
                          x + " from " + sender)
  }
}
