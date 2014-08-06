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
package tbd.list

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.{Await, ExecutionContext, Future}

import tbd.Constants._
import tbd.datastore.Datastore

class ModListInput[T, U] extends ListInput[T, U] {
  import scala.concurrent.ExecutionContext.Implicits.global

  val modList =
    new ModList[T, U](Datastore.createMod[ModListNode[T, U]](null))

  def put(key: T, value: U) {
    var innerNode = Datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]
    var previousNode: ModListNode[T, U] = null
    while (innerNode != null) {
      previousNode = innerNode
      innerNode = Datastore.getMod(innerNode.next.id)
        .asInstanceOf[ModListNode[T, U]]
    }

    val newTail = Datastore.createMod[ModListNode[T, U]](null)
    val newNode = new ModListNode((key, value), newTail)

    val futures = if (previousNode != null) {
      Datastore.updateMod(previousNode.next.id, newNode)
    } else {
      Datastore.updateMod(modList.head.id, newNode)
    }

    Await.result(Future.sequence(futures), DURATION)
  }

  def update(key: T, value: U) {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = Datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]
    var previousNode: ModListNode[T, U] = null

    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
        val newNode = new ModListNode((key, value), innerNode.next)

	if (previousNode != null) {
	  futures = Datastore.updateMod(previousNode.next.id, newNode)
	} else {
	  futures = Datastore.updateMod(modList.head.id, newNode)
	}
	found = true
      } else {
	previousNode = innerNode
        innerNode = Datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    Await.result(Future.sequence(futures), DURATION)
  }

  def remove(key: T) {
    var futures = ArrayBuffer[Future[String]]()
    var found = false
    var innerNode = Datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]

    var previousNode: ModListNode[T, U] = null
    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
        if (previousNode != null) {
          futures = Datastore.updateMod(previousNode.next.id,
                                        Datastore.getMod(innerNode.next.id))
        } else {
          futures = Datastore.updateMod(modList.head.id,
                                        Datastore.getMod(innerNode.next.id))
        }

        Datastore.removeMod(innerNode.next.id)

	found = true
      } else {
        previousNode = innerNode
        innerNode = Datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[T, U]]
      }
    }

    if (!found) {
      print("Didn't find value to remove.")
    }

    Await.result(Future.sequence(futures), DURATION)
  }

  def contains(key: T): Boolean = {
    var found = false
    var innerNode = Datastore.getMod(modList.head.id)
      .asInstanceOf[ModListNode[T, U]]

    while (innerNode != null && !found) {
      if (innerNode.value._1 == key) {
	found = true
      } else {
        innerNode = Datastore.getMod(innerNode.next.id)
          .asInstanceOf[ModListNode[T, U]]
      }
    }

    found
  }

  def getAdjustableList(): AdjustableList[T, U] = {
    modList
  }
}
