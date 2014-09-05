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
import scala.collection.immutable.TreeMap
import scala.collection.mutable.{ArrayBuffer, Map}
import scala.concurrent.{Await, ExecutionContext, Future}

import tbd.Mod
import tbd.Constants._
import tbd.datastore.Datastore

class SortedModListInput[T, U](implicit ordering: Ordering[T])
  extends ListInput[T, U] {

  import scala.concurrent.ExecutionContext.Implicits.global

  private var tailMod = Datastore.createMod[ModListNode[T, U]](null)

  var nodes = TreeMap[T, Mod[ModListNode[T, U]]]()

  val modList = new ModList[T, U](tailMod, true)

  def put(key: T, value: U) {
    val nextOption = nodes.find { case (_key, _value) => ordering.lt(key, _key) }

    nextOption match {
      case None =>
	val newTail = Datastore.createMod[ModListNode[T, U]](null)
	val newNode = new ModListNode((key, value), newTail)

	val futures = Datastore.updateMod(tailMod.id, newNode)

	nodes += ((key, tailMod))
	tailMod = newTail

	Await.result(Future.sequence(futures), DURATION)
      case Some(nextPair) =>
	val (nextKey, nextMod) = nextPair

	val nextNode = Datastore.getMod(nextMod.id).asInstanceOf[ModListNode[T, U]]
	val newNextMod = Datastore.createMod[ModListNode[T, U]](nextNode)

	val newNode = new ModListNode((key, value), newNextMod)
	val futures = Datastore.updateMod(nextMod.id, newNode)

	nodes += ((nextKey, newNextMod))
	nodes += ((key, nextMod))

	Await.result(Future.sequence(futures), DURATION)
    }
  }

  def update(key: T, value: U) {
    val nextMod = Datastore.getMod(nodes(key).id).asInstanceOf[ModListNode[T, U]].nextMod
    val newNode = new ModListNode((key, value), nextMod)

    var futures = Datastore.updateMod(nodes(key).id, newNode)

    Await.result(Future.sequence(futures), DURATION)
  }

  def remove(key: T) {
    val node = Datastore.getMod(nodes(key).id).asInstanceOf[ModListNode[T, U]]
    val nextNode = Datastore.getMod(node.nextMod.id).asInstanceOf[ModListNode[T, U]]

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = nodes(key)
    } else {
      nodes += ((nextNode.value._1, nodes(key)))
    }

    val futures = Datastore.updateMod(nodes(key).id, nextNode)

    nodes -= key

    Await.result(Future.sequence(futures), DURATION)
  }

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): AdjustableList[T, U] = {
    modList
  }
}
