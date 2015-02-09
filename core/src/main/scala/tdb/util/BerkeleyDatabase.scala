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

import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist._
import com.sleepycat.persist.model.{Entity, PrimaryKey}
import java.io.{File, FileNotFoundException}
import scala.concurrent.ExecutionContext

class BerkeleyDatabase(implicit ec: ExecutionContext) {
  private val envConfig = new EnvironmentConfig()
  envConfig.setCacheSize(96 * 1024 * 1024)
  envConfig.setAllowCreate(true)

  private val envHome = new File("/tmp/tdb_berkeleydb")
  envHome.mkdir()

  private val environment = new Environment(envHome, envConfig)

  private val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)
  private val metaStore = new EntityStore(environment, "MetaStore", storeConfig)
  private val index =
    metaStore.getPrimaryIndex(classOf[String], classOf[MetaEntity])

  def createModStore() = new BerkeleyModStore(environment)

  def createInputStore(name: String, hash: Int) = {
    if (index.contains(name)) {
      val metaEntity = index.get(name)

      new BerkeleyInputStore(environment, name, metaEntity.hash)
    } else {
      val metaEntity = new MetaEntity()
      metaEntity.storeName = name
      metaEntity.hash = hash

      index.put(metaEntity)

      new BerkeleyInputStore(environment, name, hash)
    }
  }

  def close() {
    metaStore.close()
    environment.close()
  }
}

@Entity
class MetaEntity {
  @PrimaryKey
  var storeName = ""

  var hash = -1
}
