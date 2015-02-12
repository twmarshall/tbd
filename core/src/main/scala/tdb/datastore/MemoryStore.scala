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

import akka.actor.ActorRef
import java.io._
import scala.collection.mutable.Map
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe._

import tdb.Constants.ModId
import tdb.messages.NullMessage
import tdb.util._

class MemoryStore(implicit ec: ExecutionContext) extends KVStore {
  private val tables = Map[Int, Map[Any, Any]]()

  private var nextTableId = 0

  def createTable[T: TypeTag, U: TypeTag]
      (name: String, range: HashRange): Int = {
    val id = nextTableId
    nextTableId += 1

    tables(id) = Map[Any, Any]()

    id
  }

  def load(id: Int, fileName: String) {
    val file = new File(fileName)
    val fileSize = file.length()

    val process = (key: String, value: String) => {
      //put(key, value)
      tables(id) += ((key, value))
      ()
    }

    FileUtil.readKeyValueFile(
      fileName, fileSize, 0, fileSize, process)
  }

  def put(id: Int, key: Any, value: Any) = {
    tables(id)(key) = value

    Future { "done" }
  }

  def get(id: Int, key: Any) = {
    Future {
      if (key.isInstanceOf[ModId])
        tables(id)(key)
      else
        (key, tables(id)(key))
    }
  }

  def delete(id: Int, key: Any) {
    tables(id) -= key
  }

  def contains(id: Int, key: Any): Boolean = {
    tables(id).contains(key)
  }

  def count(id: Int) = {
    tables(id).size
  }

  def clear() {
    tables.clear()
    nextTableId = 0
  }

  def hashedForeach(id: Int)(process: Iterator[Any] => Unit) {
    for (map <- tables(id).grouped(tables(id).size / 2)) {
      process(map.keys.iterator)
    }
  }

  def hashRange(id: Int) = new HashRange(0, 1, 1)
}
