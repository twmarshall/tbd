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
package tdb.datastore

import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe._

import tdb.Constants.ModId
import tdb.datastore.cassandra.CassandraStore
import tdb.worker.WorkerInfo
import tdb.util.HashRange

object KVStore {
  def apply(workerInfo: WorkerInfo)(implicit ec: ExecutionContext) =
    workerInfo.storeType  match {
      case "cassandra" => new CassandraStore(workerInfo)
      case "memory" => new MemoryStore()
    }
}

trait KVStore {
  def createTable
    (name: String, keyType: String, valueType: String, range: HashRange, recovery: Boolean): Int

  def load(id: Int, fileName: String)

  def put(id: Int, key: Any, value: Any): Future[Any]

  def get(id: Int, key: Any): Future[Any]

  def delete(id: Int, key: Any)

  def contains(id: Int, key: Any): Boolean

  def count(tableId: Int): Int

  def close()

  def processKeys(id: Int, process: Iterable[Any] => Unit)

  def foreach(id: Int)(process: (Any, Any) => Unit)

  def hashRange(id: Int): HashRange
}
