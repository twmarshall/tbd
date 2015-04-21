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

import com.datastax.driver.core.Cluster;
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
    .addContactPoint("172.19.146.4")
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

  def createTable[T: TypeTag, U: TypeTag]
      (name: String, range: HashRange): Int = {
    val id = nextStoreId
    nextStoreId += 1

    typeOf[T] match {
      case s if typeOf[T] =:= typeOf[String] && typeOf[U] =:= typeOf[String] =>
        //tables(id) = createInputStore(name, range)
      case m if typeOf[T] =:= typeOf[ModId] =>
        //tables(id) = createModStore()
      case _ => ???
    }

    id
  }

  override def close() {
    super.close()

    cluster.close()
  }
}
