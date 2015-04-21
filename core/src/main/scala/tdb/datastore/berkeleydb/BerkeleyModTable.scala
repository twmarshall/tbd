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
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey}

import tdb.Constants.ModId
import tdb.datastore._
import tdb.stats.WorkerStats
import tdb.util.Util

class BerkeleyModTable(environment: Environment) extends Table {
  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  private val store = new EntityStore(environment, "ModStore", storeConfig)
  private val primaryIndex = store.getPrimaryIndex(classOf[java.lang.Long], classOf[ModEntity])

  def put(key: Any, value: Any) {
    val entity = new ModEntity()
    entity.key = key.asInstanceOf[ModId]
    entity.value = Util.serialize(value)

    WorkerStats.berkeleyWrites += 1
    primaryIndex.put(entity)
  }

  def get(_key: Any): Any = {
    val key = _key.asInstanceOf[ModId]
    WorkerStats.berkeleyReads += 1
    val obj = Util.deserialize(primaryIndex.get(key).value)

    // No idea why this is necessary.
    if (obj != null && obj.isInstanceOf[Tuple2[_, _]]) {
      obj.toString
    }

    obj
  }

  def delete(key: Any) {
    primaryIndex.delete(key.asInstanceOf[ModId])
  }

  def contains(key: Any) = primaryIndex.contains(key.asInstanceOf[ModId])

  def count() = primaryIndex.count().toInt

  def processKeys(process: Iterable[Any] => Unit) = ???

  def hashRange = ???

  def close() {
    store.close()
  }
}

@Entity
class ModEntity {
  @PrimaryKey
  var key: Long = -1

  var value: Array[Byte] = null
}
