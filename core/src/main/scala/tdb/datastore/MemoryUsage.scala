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
package tdb.datastore

import tdb.Mod
import tdb.util.Util

object MemoryUsage {
  val INT_OVERHEAD_32 = 8
  val INT_OVERHEAD_64 = 16

  val LONG_OVERHEAD_32 = 16;
  val LONG_OVERHEAD_64 = 24;

  val OBJECT_OVERHEAD_32 = 8;
  val OBJECT_OVERHEAD_64 = 16;

  val (intOverhead,
       longOverhead,
       objectOverhead) =
    if (Util.is64) {
      (INT_OVERHEAD_64,
       LONG_OVERHEAD_64,
       OBJECT_OVERHEAD_64)
    } else {
      (INT_OVERHEAD_32,
       LONG_OVERHEAD_32,
       OBJECT_OVERHEAD_32)
    }

  // Calculates size of value, in bytes.
  def getSize(value: Any): Int = {
    value match {
      case null =>
        objectOverhead
      case s: String =>
        s.size * 2 + objectOverhead
      //case node: tdb.list.ModListNode[_, _] =>
      //  getSize(node.value) + getSize(node.nextMod) + objectOverhead
      case tuple: Tuple2[_, _] =>
        getSize(tuple._1) + getSize(tuple._2) + objectOverhead
      case i: Integer =>
        intOverhead + objectOverhead
      case m: Mod[_] =>
        getSize(m.id) + objectOverhead
      //case h: scala.collection.immutable.HashMap[_, _] =>
      //  h.size * 1000 + objectOverhead
      case l: Long =>
        longOverhead + objectOverhead

      case v: Vector[_] =>
        v.map(getSize(_)).reduce(_ + _) + objectOverhead

      // Nodes
      case node: tdb.list.DoubleListNode[_, _] =>
        getSize(node.value) + getSize(node.nextMod) + objectOverhead
      case node: tdb.list.DoubleChunkListNode[_, _] =>
        getSize(node.chunkMod) + getSize(node.nextMod) + getSize(node.size) +
        objectOverhead

      case x =>
        println(value.getClass)
        objectOverhead
    }
  }
}
