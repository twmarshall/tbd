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

import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist._
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import java.io.File
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

import tdb.Constants._
import tdb.datastore._
import tdb.util._
import tdb.worker.WorkerInfo

class BerkeleyStore(val workerInfo: WorkerInfo)
    (implicit val ec: ExecutionContext) extends CachedStore {
  private var nextStoreId = 0

  private val envConfig = new EnvironmentConfig()
  envConfig.setCacheSize(96 * 1024 * 1024)
  envConfig.setAllowCreate(true)

  private val envHome = new File(workerInfo.envHomePath)
  envHome.mkdir()

  private val environment = new Environment(envHome, envConfig)

  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)
  private val metaStore = new EntityStore(environment, "MetaStore", storeConfig)
  private val index =
    metaStore.getPrimaryIndex(classOf[String], classOf[MetaEntity])

  def createTable[T: TypeTag, U: TypeTag]
      (name: String, range: HashRange): Int = {
    val id = nextStoreId
    nextStoreId += 1

    typeOf[T] match {
      case s if typeOf[T] =:= typeOf[String] && typeOf[U] =:= typeOf[String] =>
        tables(id) = createInputStore(name, range)
      case m if typeOf[T] =:= typeOf[ModId] =>
        tables(id) = createModStore()
      case _ => ???
    }

    id
  }

  private def createModStore() = new BerkeleyModTable(environment)

  private def createInputStore(name: String, hash: HashRange) = {
    if (index.contains(name)) {
      val metaEntity = index.get(name)
      val hashRange = new HashRange(
        metaEntity.hashMin, metaEntity.hashMax, metaEntity.hashTotal)

      new BerkeleyInputTable(environment, name, hashRange)
    } else {
      val metaEntity = new MetaEntity()
      metaEntity.storeName = name
      metaEntity.hashMin = hash.min
      metaEntity.hashMax = hash.max
      metaEntity.hashTotal = hash.total

      index.put(metaEntity)

      new BerkeleyInputTable(environment, name, hash)
    }
  }

  override def clear() {
    super.clear()

    nextStoreId = 0

    metaStore.close()
    environment.close()
  }
}

@Entity
class MetaEntity {
  @PrimaryKey
  var storeName = ""

  var hashMin = -1
  var hashMax = -1
  var hashTotal = -1
}
