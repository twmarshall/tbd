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
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import java.io._

class BerkeleyInputStore(environment: Environment, name: String) {
  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  private val store = new EntityStore(environment, name, storeConfig)
  val primaryIndex = store.getPrimaryIndex(classOf[Integer], classOf[InputEntity])

  def put(key: Int, value: (String, String)) {
    val entity = new InputEntity()
    entity.key = key
    entity.title = value._1
    entity.value = value._2

    primaryIndex.put(entity)
  }

  def get(key: Integer): (String, String) = {
    val entity = primaryIndex.get(key)
    (entity.title, entity.value)
  }

  def delete(key: Integer) {
    primaryIndex.delete(key)
  }

  def keys() = primaryIndex.keys()

  def contains(key: Integer) = primaryIndex.contains(key)

  def count() = primaryIndex.count()

  def close() {
    store.close()
  }
}

@Entity
class InputEntity {
  @PrimaryKey
  var key: java.lang.Integer = -1

  var title: String = ""

  var value: String = ""
}
