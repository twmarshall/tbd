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
import com.sleepycat.persist._
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import com.sleepycat.persist.model.Relationship.ONE_TO_ONE
import java.io._
import scala.collection.JavaConversions._
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.ExecutionContext

trait BerkeleyStore {
  def load(fileName: String)

  def put(key: Any, value: Any)

  def get(key: Any): Any

  def delete(key: Any)

  def contains(key: Any): Boolean

  def count(): Int

  def hashedForeach(process: Iterator[String] => Unit)

  def hashRange: HashRange

  def close()
}

class BerkeleyInputStore
    (environment: Environment, name: String, val hashRange: HashRange)
    (implicit ec: ExecutionContext) extends BerkeleyStore {
  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  private val stores = Buffer[EntityStore]()
  private val indexes = Buffer[PrimaryIndex[String, InputEntity]]()

  for (i <- hashRange.range()) {
    val store = new EntityStore(environment, name + i, storeConfig)
    stores += store

    val index = store.getPrimaryIndex(classOf[String], classOf[InputEntity])
    indexes += index
  }

  private val hasher = ObjHasher.makeHasher(hashRange, indexes)

  def load(fileName: String) {
    val file = new File(fileName)
    val fileSize = file.length()

    val process = (key: String, value: String) => {
      if (hashRange.fallsInside(key)) {
        put(key, value)
      }
    }

    FileUtil.readKeyValueFile(
      fileName, fileSize, 0, fileSize, process)
  }

  def put(key: Any, value: Any) {
    val entity = new InputEntity()
    entity.key = key.asInstanceOf[String]
    entity.value = value.asInstanceOf[String]

    hasher.getObj(key).put(entity)
  }

  def get(_key: Any) = {
    val key = _key.asInstanceOf[String]
    val entity = hasher.getObj(key).get(key)
    (key, entity.value)
  }

  def delete(key: Any) = ???

  def contains(key: Any) =
    indexes.map(_.contains(key.asInstanceOf[String])).reduce(_ || _)

  def count(): Int = indexes.map(_.count().toInt).reduce(_ + _)

  def hashedForeach(process: Iterator[String] => Unit) = {
    for (index <- indexes) {
      val cursor = index.keys()
      process(cursor.iterator)
      cursor.close()
    }
  }

  def foreachPartition(func: PrimaryIndex[String, InputEntity] => Unit) {
    for (index <- indexes) {
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
