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

class CassandraModTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange) extends CassandraTable {
  //session.execute("""DROP TABLE IF EXISTS tdb.mods;""")
  val query = """CREATE TABLE IF NOT EXISTS tdb.mods
    (key bigint, value blob, PRIMARY KEY(key));"""
  session.execute(query)

  private val putStmt = session.prepare(
    s"""INSERT INTO $tableName (key, value) VALUES (?, ?);""")

  private val getStmt = session.prepare(
    s"""SELECT * FROM $tableName WHERE key = ?""")

  private val deleteStmt = session.prepare(
    s"""DELETE FROM $tableName WHERE key = ?""")

  private val containsStmt = session.prepare(
    s"""SELECT COUNT(*) FROM $tableName WHERE key = ?""")

  def put(key: Any, value: Any) {
    val serializedValue =
      java.nio.ByteBuffer.wrap(Util.serialize(value))
    val stmt = new BoundStatement(putStmt)

    session.execute(stmt.bind(key.asInstanceOf[Object], serializedValue))
  }

  def get(key: Any): Any = {
    val stmt = new BoundStatement(getStmt)
    val results = session.execute(stmt.bind(key.asInstanceOf[Object]))
    val byteBuffer = results.one().getBytes("value")
    Util.deserialize(byteBuffer.array())
  }

  def delete(key: Any) = {
    val stmt = new BoundStatement(getStmt)
    session.execute(stmt.bind(key.asInstanceOf[Object]))
  }

  def contains(key: Any): Boolean = {
    val stmt = new BoundStatement(containsStmt)
    val results = session.execute(stmt.bind(key.asInstanceOf[Object]))
    results.one().getLong("count") > 0
  }

  def processKeys(process: Iterable[Any] => Unit) = ???

  def close() {}
}
