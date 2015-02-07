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
package tdb.util

import com.sleepycat.je.Environment
import com.sleepycat.persist.{EntityStore, PrimaryIndex, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import java.io._
import scala.collection.mutable.Buffer

class BerkeleyInputStore(environment: Environment, name: String) {
  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  //private val store = new EntityStore(environment, name, storeConfig)
  //val primaryIndex = store.getPrimaryIndex(classOf[String], classOf[InputEntity])

  private val stores = Buffer[EntityStore]()
  private val indexes = Buffer[PrimaryIndex[String, InputEntity]]()

  for (i <- 1 to 12) {
    val store = new EntityStore(environment, name + i, storeConfig)

    stores += store
    indexes += store.getPrimaryIndex(classOf[String], classOf[InputEntity])
  }

  private def getPrimaryIndex(title: String) = {
    indexes(title.hashCode().abs % indexes.size)
  }

  def put(key: String, value: String) {
    val entity = new InputEntity()
    entity.key = key
    entity.value = value

    getPrimaryIndex(key).put(entity)
  }

  def get(key: String) = {
    val entity = getPrimaryIndex(key).get(key)
    (entity.key, entity.value)
  }

  def delete(key: String) {
    getPrimaryIndex(key).delete(key)
  }

  def keys() = indexes.map(_.keys())

  def contains(key: String) = getPrimaryIndex(key).contains(key)

  def count() = indexes.map(_.count()).reduce(_ + _)

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
