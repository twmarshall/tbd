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
package tdb.datastore.cassandra

import com.datastax.driver.core.Cluster
import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext
import scala.reflect.runtime.universe._

import tdb.Constants._
import tdb.datastore._
import tdb.util._
import tdb.worker.WorkerInfo

class CassandraStore(val workerInfo: WorkerInfo)
    (implicit val ec: ExecutionContext) extends CachedStore {
  private var nextStoreId = 0

  private val cluster = Cluster.builder()
    .addContactPoint(workerInfo.ip)
    .build()

  private val metadata = cluster.getMetadata()
  printf("Connected to cluster: %s\n",
                    metadata.getClusterName())
  for (host <- metadata.getAllHosts()) {
    printf("Datatacenter: %s; Host: %s; Rack: %s\n",
           host.getDatacenter(), host.getAddress(), host.getRack())
  }

  private val session = cluster.connect()
  session.execute(
    """CREATE KEYSPACE IF NOT EXISTS tdb WITH replication =
    {'class':'SimpleStrategy', 'replication_factor':3};""")

  def createTable
      (name: String,
       keyType: String,
       valueType: String,
       range: HashRange,
       recovery: Boolean): Int = {
    val id = nextStoreId
    nextStoreId += 1

    keyType match {
      case "String" =>
        valueType match {
          case "Double" =>
            tables(id) = new CassandraStringDoubleTable(
              session, convertName(name), range, recovery)
          case "Int" =>
            tables(id) = new CassandraStringIntTable(
              session, convertName(name), range, recovery)
          case "Long" =>
            tables(id) = new CassandraStringLongTable(
              session, convertName(name), range, recovery)
          case "String" =>
            tables(id) = new CassandraStringStringTable(
              session, convertName(name), range, recovery)
        }
      case "ModId" =>
        tables(id) = new CassandraModTable(session, "tdb.mods", range)
    }

    id
  }

  private def convertName(name: String) =
    "tdb." + name.replace("/", "_").replace(".", "_").replace("-", "_")

  override def close() {
    super.close()

    cluster.close()
  }
}
