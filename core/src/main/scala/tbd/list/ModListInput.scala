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

import scala.collection.mutable.Map

import tbd.{Mod, Mutator}

class ModListInput[T, U](mutator: Mutator) extends ListInput[T, U] {
  private var tailMod = mutator.createMod[ModListNode[T, U]](null)

  val nodes = Map[T, Mod[ModListNode[T, U]]]()

  val modList = new ModList[T, U](tailMod)

  def load(data: Map[T, U]) {
    var tail = mutator.createMod[ModListNode[T, U]](null)
    val newTail = tail

    for ((key, value) <- data) {
      tail = mutator.createMod(new ModListNode((key, value), tail))
      nodes(key) = tail
    }

    val head = mutator.read(tail)
    mutator.updateMod(tailMod, head)
    nodes(head.value._1) = tailMod
    tailMod = newTail
  }

  def put(key: T, value: U) {
    val newTail = mutator.createMod[ModListNode[T, U]](null)
    val newNode = new ModListNode((key, value), newTail)

    mutator.updateMod(tailMod, newNode)

    nodes(key) = tailMod
    tailMod = newTail
  }

  def putAfter(key: T, pair: (T, U)) {
    val before = mutator.read(nodes(key))

    val newNode = new ModListNode(pair, before.nextMod)
    val newNodeMod = mutator.createMod(newNode)

    val newBefore = new ModListNode(before.value, newNodeMod)
    mutator.updateMod(nodes(key), newBefore)

    nodes(pair._1) = newNodeMod
  }

  def update(key: T, value: U) {
    val nextMod = mutator.read(nodes(key)).nextMod
    val newNode = new ModListNode((key, value), nextMod)

    mutator.updateMod(nodes(key), newNode)
  }

  def remove(key: T) {
    val node = mutator.read(nodes(key))
    val nextNode = mutator.read(node.nextMod)

    if (nextNode == null) {
      // We're removing the last element in the last.
      assert(tailMod == node.nextMod)
      tailMod = nodes(key)
    } else {
      nodes(nextNode.value._1) = nodes(key)
    }

    mutator.updateMod(nodes(key), nextNode)

    nodes -= key
  }

  def contains(key: T): Boolean = {
    nodes.contains(key)
  }

  def getAdjustableList(): AdjustableList[T, U] = {
    modList
  }
}
