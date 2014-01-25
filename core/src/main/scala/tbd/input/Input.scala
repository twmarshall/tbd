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
package tbd.input

import akka.actor.{Actor, ActorRef, ActorLogging}
import scala.collection.mutable.Map

import tbd.ListNode
import tbd.messages._
import tbd.mod.{Matrix, Mod}

class Input(modStoreRef: ActorRef) extends Actor with ActorLogging {
  val tables = Map[String, Map[Int, Any]]()

  def createTable(table: String) {
    println("create table" + table)
    tables(table) = Map[Int, Any]()
  }

  def get(table: String, key: Int): Mod[Any] =
    new Mod(tables(table)(key), modStoreRef)

  def put(table: String, key: Int, value: Any) {
    tables(table)(key) = value
  }

  /*def putArray(key: Int, value: Array[Any]) {
    val modArray = value.map(elem => new Mod(elem, modStoreRef))
  }*/

  def putMatrix(table: String, key: Int, value: Array[Array[Int]]) {
    tables(table)(key) = new Matrix(value, modStoreRef)
  }

  def asArray(table: String): Array[Mod[Any]] = {
    val arr = new Array[Mod[Any]](tables(table).size)

    var i = 0
    for (elem <- tables(table)) {
      arr(i) = new Mod[Any](elem._2, modStoreRef)
      i += 1
    }

    arr
  }

  def asList(table: String): Mod[ListNode[Any]] = {
    var head = new Mod[ListNode[Any]](null, modStoreRef)

    for (elem <- tables(table)) {
      val value = new Mod(elem._2, modStoreRef)
      head = new Mod(new ListNode(value, head), modStoreRef)
    }

    head
  }

  def receive = {
    case CreateTableMessage(table: String) =>
      createTable(table)
    case GetMessage(table: String, key: Int) =>
      sender ! get(table, key)
    case PutMessage(table: String, key: Int, value: Any) =>
      put(table, key, value)
    case PutMatrixMessage(table: String, key: Int, value: Array[Array[Int]]) =>
      putMatrix(table, key, value)
    //case GetSizeMessage => sender ! data.size
    case GetArrayMessage(table: String) => sender ! asArray(table)
    case GetListMessage(table: String) => sender ! asList(table)
    case _ => log.warning("Input actor received unkown message from " + sender)
  }
}
