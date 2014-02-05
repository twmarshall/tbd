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
package tbd.ddg

import scala.collection.mutable.Set

import tbd.mod.ModId

abstract class Node(aModId: ModId, aParent: Node) {
  val children = Set[Node]()
  val modId: ModId = aModId
  val parent = aParent

  def addChild(child: Node) {
    children += child
  }

  def name(): String

  def toString(prefix: String): String = {
    val childrenString =
      if (children.isEmpty) {
	""
      } else if (children.size == 1) {
	"\n" + children.head.toString(prefix + "-")
      } else {
	"\n" + children.map(_.toString(prefix + "-")).reduceLeft(_ + "\n" + _)
      }

    prefix + name() + "(" + modId + ")" + childrenString
  }
}

class ReadNode(aModId: ModId, aParent: Node) extends Node(aModId, aParent) {
  var updated = false

  def name() = "ReadNode (" + updated + ")"
}

class WriteNode(aModId: ModId, aParent: Node) extends Node(aModId, aParent) {
  def name() = "WriteNode"
}

class RootNode extends Node(null, null) {
  def name() = "RootNode"
}
