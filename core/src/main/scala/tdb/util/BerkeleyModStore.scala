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

import tdb.Constants.ModId

class BerkeleyModStore(environment: Environment) {
  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  private val store = new EntityStore(environment, "ModStore", storeConfig)
  val primaryIndex = store.getPrimaryIndex(classOf[java.lang.Long], classOf[ModEntity])

  def put(key: ModId, value: Any) {
    val entity = new ModEntity()
    entity.key = key

    val byteOutput = new ByteArrayOutputStream()
    val objectOutput = new ObjectOutputStream(byteOutput)
    objectOutput.writeObject(value)
    entity.value = byteOutput.toByteArray

    primaryIndex.put(entity)
  }

  def get(key: ModId): Any = {
    val byteArray = primaryIndex.get(key).value

    val byteInput = new ByteArrayInputStream(byteArray)
    val objectInput = new ObjectInputStream(byteInput)
    val obj = objectInput.readObject()

    // No idea why this is necessary.
    if (obj != null && obj.isInstanceOf[Tuple2[_, _]]) {
      obj.toString
    }

    obj
  }

  def delete(key: ModId) {
    primaryIndex.delete(key)
  }

  def keys() = primaryIndex.keys()

  def contains(key: ModId) = primaryIndex.contains(key)

  def count() = primaryIndex.count()

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
