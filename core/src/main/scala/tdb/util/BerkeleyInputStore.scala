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
import com.sleepycat.persist.{EntityStore, PrimaryIndex, SecondaryIndex, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey, SecondaryKey}
import java.io._
import scala.collection.mutable.{Buffer, Map}

class BerkeleyInputStore(environment: Environment, name: String) {
  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  private val stores = Buffer[EntityStore]()
  private val indexes = Buffer[PrimaryIndex[Integer, InputEntity]]()
  private val keyStores = Buffer[EntityStore]()
  private val keyIndexes = Buffer[PrimaryIndex[Integer, KeyEntity]]()

  private val keys = Map[String, Integer]()

  for (i <- 1 to 12) {
    val store = new EntityStore(environment, name + i, storeConfig)
    stores += store

    indexes += store.getPrimaryIndex(classOf[Integer], classOf[InputEntity])

    val keyStore = new EntityStore(environment, name + "Key" + i, storeConfig)
    stores += keyStore

    keyIndexes += keyStore.getPrimaryIndex(classOf[Integer], classOf[KeyEntity])
  }

  private def hashKey(key: String) = {
    key.hashCode().abs % indexes.size
  }

  var nextId = 0
  def put(key: String, value: String) {
    val entity = new InputEntity()
    entity.id = nextId
    nextId += 1
    entity.value = value

    val hash = hashKey(key)
    indexes(hash).put(entity)

    val keyEntity = new KeyEntity()
    keyEntity.id = entity.id
    keyEntity.key = key

    keyIndexes(hash).put(keyEntity)

    keys(key) = entity.id
  }

  def get(key: String) = {
    val entity = indexes(hashKey(key)).get(keys(key))
    (key, entity.value)
  }

  def delete(key: String) {
    indexes(hashKey(key)).delete(keys(key))
  }

  def iterateInput(process: Iterable[String] => Unit, partitions: Int) {
    val cursors = keyIndexes.map(_.entities())

    val buf = Buffer[String]()
    for (cursor <- cursors) {
      val it = cursor.iterator
      while (it.hasNext) {
        val keyEntity = it.next()
        buf += keyEntity.key
        keys(keyEntity.key) = keyEntity.id
      }

      process(buf)
      buf.clear()

      cursor.close()
    }
  }

  def contains(key: String) = indexes(hashKey(key)).contains(keys(key))

  def count() = indexes.map(_.count()).reduce(_ + _)

  def close() {
    for (store <- stores) {
      store.close()
    }
  }
}

@Entity
class KeyEntity {
  @PrimaryKey
  var id: Integer = -1

  var key: String = ""
}

@Entity
class InputEntity {
  @PrimaryKey
  var id: Integer = -1

  var value: String = ""
}
