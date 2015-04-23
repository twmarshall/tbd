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
package tdb.datastore.berkeleydb

import com.sleepycat.je.Environment
import com.sleepycat.persist._
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import java.io.File
import scala.collection.JavaConversions._
import scala.collection.mutable.{Buffer, Map}

import tdb.datastore._
import tdb.stats.WorkerStats
import tdb.util._

class BerkeleyInputTable
    (environment: Environment, name: String, val hashRange: HashRange)
  extends Table {

  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  private val stores = Buffer[EntityStore]()
  private val indexes = Map[Int, PrimaryIndex[String, InputEntity]]()

  for (i <- hashRange.range()) {
    val store = new EntityStore(environment, name + i, storeConfig)
    stores += store

    val index = store.getPrimaryIndex(classOf[String], classOf[InputEntity])
    indexes(i) = index
  }

  private val hasher = new ObjHasher(indexes, hashRange.total)

  def put(key: Any, value: Any) {
    val entity = new InputEntity()
    entity.key = key.asInstanceOf[String]
    entity.value = value.asInstanceOf[String]

    WorkerStats.berkeleyWrites += 1
    hasher.getObj(key).put(entity)
  }

  def get(_key: Any) = {
    val key = _key.asInstanceOf[String]

    WorkerStats.berkeleyReads += 1
    val entity = hasher.getObj(key).get(key)
    entity.value
  }

  def delete(key: Any) = ???

  def contains(key: Any) =
    indexes.values.map(_.contains(key.asInstanceOf[String])).reduce(_ || _)

  def count(): Int = indexes.values.map(_.count().toInt).reduce(_ + _)

  def processKeys(process: Iterable[Any] => Unit) = {
    val cursor = indexes.head._2.keys()
    process(cursor)
    cursor.close()
  }

  def foreach(process: (Any, Any) => Unit) = ???

  def foreachPartition(func: PrimaryIndex[String, InputEntity] => Unit) {
    for (index <- indexes.values) {
      func(index)
    }
  }

  def close() {
    for (store <- stores) {
      store.close()
    }
  }
}

@Entity
class InputEntity {
  @PrimaryKey
  var key: String = ""

  var value: String = ""
}
