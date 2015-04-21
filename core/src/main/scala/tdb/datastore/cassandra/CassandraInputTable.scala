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

import com.datastax.driver.core.{BoundStatement, Session}
import scala.collection.JavaConversions._

import tdb.datastore._
import tdb.util._

class CassandraInputTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange) extends CassandraTable {

  val query = s"""CREATE TABLE IF NOT EXISTS $tableName
    (key text, value text, PRIMARY KEY(key));"""
  session.execute(query)

  private val putStmt = session.prepare(
    s"""INSERT INTO $tableName (key, value) VALUES (?, ?);""")

  private val getStmt = session.prepare(
    s"""SELECT * FROM $tableName WHERE key = ?""")

  def put(key: Any, value: Any) {
    val stmt = new BoundStatement(putStmt)

    session.execute(stmt.bind(key.asInstanceOf[Object], value.asInstanceOf[Object]))
  }

  def get(key: Any): Any = {
    val stmt = new BoundStatement(getStmt)
    val results = session.execute(stmt.bind(key.asInstanceOf[Object]))
    (key, results.one().getString("value"))
  }

  def delete(key: Any) = ???

  def contains(key: Any): Boolean = ???

  def processKeys(process: Iterable[Any] => Unit) = {
    val stmt = s"""SELECT * FROM $tableName"""
    val results = session.execute(stmt)
    process(results.all().map(_.getString("key")))
  }

  def close() {}
}
