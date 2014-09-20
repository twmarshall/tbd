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

import tbd.{Mod, Mutator}
import tbd.Constants._
import tbd.datastore.Datastore

class SortedModListInput[T, U](mutator: Mutator)(implicit ordering: Ordering[T])
  extends ListInput[T, U] {

  import scala.concurrent.ExecutionContext.Implicits.global

  private var tailMod = mutator.createMod[ModListNode[T, U]](null)

  var nodes = TreeMap[T, Mod[ModListNode[T, U]]]()

  val modList = new ModList[T, U](tailMod, true)

  def put(key: T, value: U) {
    val nextOption = nodes.find { case (_key, _value) => ordering.lt(key, _key) }

    nextOption match {
      case None =>
	val newTail = mutator.createMod[ModListNode[T, U]](null)
	val newNode = new ModListNode((key, value), newTail)

        val future = tailMod.update(newNode)

	nodes += ((key, tailMod))
	tailMod = newTail

	Await.result(future, DURATION)
      case Some(nextPair) =>
	val (nextKey, nextMod) = nextPair

        val nextNode = nextMod.read()
	val newNextMod = mutator.createMod[ModListNode[T, U]](nextNode)

	val newNode = new ModListNode((key, value), newNextMod)
        val future = nextMod.update(newNode)

	nodes += ((nextKey, newNextMod))
	nodes += ((key, nextMod))

	Await.result(future, DURATION)
    }
  }

  // Note: doesn't really make sense to allow random insertions into a sorted
  // list, so we just ignore the key.
  def putAfter(key: T, newPair: (T, U)) {
    put(newPair._1, newPair._2)
  }

  def update(key: T, value: U) {
    val nextMod = nodes(key).read().nextMod
    val newNode = new ModListNode((key, value), nextMod)

    val future = nodes(key).update(newNode)

    Await.result(future, DURATION)
  }

  def remove(key: T) {
    val node = nodes(key).read()
    val nextNode = node.nextMod.read()

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = nodes(key)
    } else {
      nodes += ((nextNode.value._1, nodes(key)))
    }

    val future = nodes(key).update(nextNode)

    nodes -= key

    Await.result(future, DURATION)
  }

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): AdjustableList[T, U] = {
    modList
  }
}