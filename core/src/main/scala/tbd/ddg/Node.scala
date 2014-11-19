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

import akka.actor.ActorRef
import akka.pattern.ask
import scala.concurrent.Await

import tbd._
import tbd.Constants._

object Node {
  var id = 0

  def getId(): Int = {
    id = id + 1
    id
  }
}

object ReadNode {
  private val modIdOffset = 0
  private val updatedOffset = 8

  def create(modId: ModId, updated: Boolean): Pointer[ReadNode] = {
    val ptr = MemoryAllocator.allocate(8 * 3 + 1)

    MemoryAllocator.unsafe.putLong(ptr + modIdOffset, modId)

    val byte = if (updated) 1.toByte else 0.toByte

    MemoryAllocator.unsafe.putByte(ptr + updatedOffset, byte)

    ptr
  }

  def getModId(ptr: Pointer[ReadNode]): ModId = {
    MemoryAllocator.unsafe.getLong(ptr)
  }

  def getUpdated(ptr: Pointer[ReadNode]): Boolean = {
    val byte = MemoryAllocator.unsafe.getByte(ptr + updatedOffset)

    byte == 1
  }

  def setUpdated(ptr: Pointer[ReadNode], updated: Boolean) {
    val byte = if (updated) 1.toByte else 0.toByte

    MemoryAllocator.unsafe.putByte(ptr + updatedOffset, byte)
  }
}

abstract class Node {
  var currentModId: ModId = -1

  var currentModId2: ModId = -1

  var updated = false
}

class MemoNode
    (val signature: Seq[Any],
     val memoizer: Memoizer[_]) extends Node {

  var value: Any = null
}

class ModNode
    (val modId1: ModId,
     val modId2: ModId,
     val modizer: Modizer[Any],
     val key: Any) extends Node

class ParNode
    (val taskRef1: ActorRef,
     val taskRef2: ActorRef) extends Node {
  var pebble1 = false
  var pebble2 = false
}

class ReadNode(val reader: Any => Changeable[Any]) extends Node

class Read2Node
    (val modId1: ModId,
     val modId2: ModId,
     val reader: (Any, Any) => Changeable[Any]) extends Node

class RootNode extends Node

class WriteNode(val modId: ModId, val modId2: ModId) extends Node
