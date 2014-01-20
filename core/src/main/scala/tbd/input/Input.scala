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
  val data = Map[Int, Any]()

  def get(key: Int): Mod[Any] =
    new Mod(data(key), modStoreRef)

  def put(key: Int, value: Any) {
    data(key) = value
  }

  def putArray(key: Int, value: Array[Any]) {
    val modArray = value.map(elem => new Mod(elem, modStoreRef))
  }

  def putMatrix(key: Int, value: Array[Array[Int]]) {
    data(key) = new Matrix(value, modStoreRef)
  }

  def asArray(): Array[Mod[Any]] = {
    val arr = new Array[Mod[Any]](data.size)

    var i = 0
    for (elem <- data) {
      arr(i) = new Mod[Any](elem._2, modStoreRef)
      i += 1
    }

    arr
  }

  def asList(): Mod[ListNode[Any]] = {
    var head = new Mod[ListNode[Any]](null, modStoreRef)

    for (elem <- data) {
      val value = new Mod(elem._2, modStoreRef)
      head = new Mod(new ListNode(value, head), modStoreRef)
    }

    head
  }

  def receive = {
    case GetMessage(key: Int) => sender ! get(key)
    case PutMessage(key: Int, value: Any) => put(key, value)
    case PutMatrixMessage(key: Int, value: Array[Array[Int]]) => putMatrix(key, value)
    case GetSizeMessage => sender ! data.size
    case GetArrayMessage => sender ! asArray()
    case GetListMessage => sender ! asList()
    case _ => log.warning("Input actor received unkown message from " + sender)
  }
}
