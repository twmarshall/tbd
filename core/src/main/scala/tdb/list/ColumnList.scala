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
import scala.collection.mutable.{Buffer, Map}

import tdb._
import tdb.Constants._
import tdb.TDB._

object ColumnList {
  type Columns = Map[String, Any]
  type ModColumns = Map[String, Mod[Iterable[Any]]]
}

import ColumnList._

class ColumnList[T]
    (val head: Mod[ColumnListNode[T]],
     conf: ListConf,
     val sorted: Boolean = false,
     val workerId: WorkerId = -1)
  extends AdjustableList[T, Columns] with Serializable {

  def filter(pred: ((T, Columns)) => Boolean)
      (implicit c: Context): DoubleChunkList[T, Columns] = ???

  def flatMap[V, W](f: ((T, Columns)) => Iterable[(V, W)])
      (implicit c: Context): DoubleChunkList[V, W] = ???

  def join[V](_that: AdjustableList[T, V], condition: ((T, V), (T, Columns)) => Boolean)
      (implicit c: Context): DoubleChunkList[T, (Columns, V)] = ???

  def map[V, W](f: ((T, Columns)) => (V, W))
      (implicit c: Context): DoubleChunkList[V, W] = ???

  def merge(that: DoubleChunkList[T, Columns])
      (implicit c: Context,
       ordering: Ordering[T]): DoubleChunkList[T, Columns] = ???

  def merge
      (that: DoubleChunkList[T, Columns],
       memo: Memoizer[Changeable[DoubleChunkListNode[T, Columns]]],
       modizer: Modizer1[DoubleChunkListNode[T, Columns]])
      (implicit c: Context,
       ordering: Ordering[T]): DoubleChunkList[T, Columns] = ???

  override def projection2
      (column1: String, column2: String, f: (T, Any, Any, Context) => Unit)
      (implicit c: Context): Unit = {
    val memo = new Memoizer[Unit]()

    readAny(head) {
      case null =>
      case node => node.projection2(column1, column2, f, memo)
    }
  }

  def reduce(f: ((T, Columns), (T, Columns)) => (T, Columns))
      (implicit c: Context): Mod[(T, Columns)] = ???

  def sortJoin[V](_that: AdjustableList[T, V])
      (implicit c: Context,
       ordering: Ordering[T]): AdjustableList[T, (Columns, V)] = ???

  def split(pred: ((T, Columns)) => Boolean)
      (implicit c: Context): (AdjustableList[T, Columns], AdjustableList[T, Columns]) = ???

  def toBuffer(mutator: Mutator): Buffer[(T, Map[String, Any])] = {
    val buf = Buffer[(T, Map[String, Any])]()
    var node = mutator.read(head)

    while (node != null) {
      val columns = Buffer[Map[String, Any]]()

      val keyChunk = mutator.read(node.columns("key"))
      val iter = columns.iterator
      for (k <- keyChunk) {
        val m = Map[String, Any]()
        buf += ((k.asInstanceOf[T], m))
        columns += m
      }

      for ((columnName, chunkMod) <- node.columns) {
        if (columnName != "key") {
          val chunk = mutator.read(chunkMod)
          val iter = columns.iterator
          for (v <- chunk) {
            val m = iter.next
            m(columnName) = v
          }
        }
      }

      node = mutator.read(node.nextMod)
    }

    buf
  }

  override def equals(that: Any): Boolean = {
    that match {
      case thatList: DoubleChunkList[T, _] => head == thatList.head
      case _ => false
    }
  }

  override def hashCode() = head.hashCode()

  override def toString: String = {
    head.toString
  }
}
