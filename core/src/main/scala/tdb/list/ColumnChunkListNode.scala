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

import java.io.Serializable
import scala.collection.immutable
import scala.collection.mutable.Map

import tdb._
import tdb.TDB._

class ColumnChunkListNode[T]
    (val columns: Map[String, Mod[Iterable[Any]]],
     val nextMod: Mod[ColumnChunkListNode[T]],
     val size: Int = 0) extends Serializable {

  def projection2
      (column1: String,
       column2: String,
       f: (T, Any, Any, Context) => Unit,
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    read_3(columns("key"), columns(column1), columns(column2)) {
      case (keyValues, column1Values, column2Values) =>
        val keyIter = keyValues.iterator
        val column1Iter = column1Values.iterator
        val column2Iter = column2Values.iterator
        while (keyIter.hasNext) {
          f(keyIter.next.asInstanceOf[T], column1Iter.next, column2Iter.next, c)
        }
    }

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.projection2(column1, column2, f, memo)
        }
    }
  }

  def projection2Chunk
      (column1: String,
       column2: String,
       f: (Iterable[T], Iterable[Any], Iterable[Any], Context) => Unit,
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    read_3(columns("key"), columns(column1), columns(column2)) {
      case (keyValues, column1Values, column2Values) =>
        f(keyValues.asInstanceOf[Iterable[T]], column1Values, column2Values, c)
    }

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.projection2Chunk(column1, column2, f, memo)
        }
    }
  }

  def foreach[V, W]
      (f: (Map[String, Mod[Iterable[Any]]], Context) => Unit,
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    f(columns, c)

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.foreach(f, memo)
        }
    }
  }

  /*override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ColumnListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[ColumnListNode[T, U]]
      that.chunkMod == chunkMod && that.nextMod == nextMod
    }
  }

  override def hashCode() = chunkMod.hashCode() * nextMod.hashCode()

  override def toString = "Node(" + chunkMod + ", " + nextMod + ")"*/
}
