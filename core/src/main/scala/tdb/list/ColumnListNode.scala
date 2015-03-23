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

class ColumnListNode[T]
    (val key: T,
     val columns: Map[String, Mod[Any]],
     val nextMod: Mod[ColumnListNode[T]],
     val size: Int = 0) extends Serializable {

  def projection2
      (column1: String,
       column2: String,
       f: (T, Any, Any, Context) => Unit,
       memo: Memoizer[Unit],
       input: ColumnListInput[T])
      (implicit c: Context): Unit = {
    getFrom(input, (key, Iterable(column1, column2))) {
      case values =>
        val iter = values.iterator
        val keyValue = iter.next.asInstanceOf[T]
        val column1Value = iter.next
        val column2Value = iter.next
        f(keyValue, column1Value, column2Value, c)
    }

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.projection2(column1, column2, f, memo, input)
        }
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case that: ColumnListNode[T] =>
      that.key == this.key && that.nextMod == this.nextMod
      case _ => false
    }
  }

  override def hashCode() = key.hashCode() * nextMod.hashCode()

  //override def toString = "Node(" + chunkMod + ", " + nextMod + ")"
}
