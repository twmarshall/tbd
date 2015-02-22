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
package tdb.list

import scala.collection.immutable.TreeMap
import scala.collection.mutable.Map
import scala.concurrent.Await

import tdb.{Mod, Mutator}

abstract class SortedModListInput[T, U](mutator: Mutator)(implicit ordering: Ordering[T])
  extends ListInput[T, U] {

  private var tailMod = mutator.createMod[ModListNode[T, U]](null)

  var nodes = TreeMap[T, Mod[ModListNode[T, U]]]()

  val modList = new ModList[T, U](tailMod, true)

  def load(data: Map[T, U]) {
    var sortedData = data.toBuffer.sortWith {
      (pair1: (T, U), pair2: (T, U)) => !ordering.lt(pair1._1, pair2._1)
    }

    var tail = mutator.createMod[ModListNode[T, U]](null)
    val newTail = tail

    while (sortedData.size > 1) {
      tail = mutator.createMod(new ModListNode(sortedData.head, tail))
      nodes += ((sortedData.head._1, tail))
      sortedData = sortedData.tail
    }

    mutator.updateMod(tailMod, new ModListNode(sortedData.head, tail))
    nodes += ((sortedData.head._1, tailMod))
    tailMod = newTail
  }


  def asyncPut(key: T, value: U) = ???

  def asyncPutAll(values: Iterable[(T, U)]) = ???

  def put(key: T, value: U) {
    val nextOption =
      nodes.find { case (_key, _value) => ordering.lt(key, _key) }

    nextOption match {
      case None =>
        val newTail = mutator.createMod[ModListNode[T, U]](null)
        val newNode = new ModListNode((key, value), newTail)

        mutator.updateMod(tailMod, newNode)

        nodes += ((key, tailMod))
        tailMod = newTail
      case Some(nextPair) =>
        val (nextKey, nextMod) = nextPair

        val nextNode = mutator.read(nextMod)
        val newNextMod = mutator.createMod[ModListNode[T, U]](nextNode)

        val newNode = new ModListNode((key, value), newNextMod)
        mutator.updateMod(nextMod, newNode)

        nodes += ((nextKey, newNextMod))
        nodes += ((key, nextMod))
    }
  }

  def update(key: T, value: U) {
    val nextMod = mutator.read(nodes(key)).nextMod
    val newNode = new ModListNode((key, value), nextMod)

    mutator.updateMod(nodes(key), newNode)
  }

  def remove(key: T, value: U) {
    val node = mutator.read(nodes(key))
    val nextNode = mutator.read(node.nextMod)

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = nodes(key)
    } else {
      nodes += ((nextNode.value._1, nodes(key)))
    }

    mutator.updateMod(nodes(key), nextNode)

    nodes -= key
  }

  def removeAll(values: Iterable[(T, U)]) = ???

  def asyncRemove(key: T, value: U) = ???

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): AdjustableList[T, U] = {
    modList
  }
}
