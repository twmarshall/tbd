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

import com.datastax.driver.core.{BoundStatement, Row, Session}
import com.datastax.driver.core.utils.Bytes
import java.nio.ByteBuffer
import java.util.concurrent.locks.ReentrantLock
import scala.collection.JavaConversions._

import tdb.datastore.Table
import tdb.util._

object Cassandra {
  val lock = new ReentrantLock()
}

trait CassandraTable extends Table {
  Cassandra.lock.lock()

  initialize()

  private val getStmt = session.prepare(
    s"""SELECT * FROM $tableName WHERE key = ?""")

  private val putStmt = session.prepare(
    s"""INSERT INTO $tableName (key, value) VALUES (?, ?);""")

  private val deleteStmt = session.prepare(
    s"""DELETE FROM $tableName WHERE key = ?""")

  private val containsStmt = session.prepare(
    s"""SELECT COUNT(*) FROM $tableName WHERE key = ?""")

  Cassandra.lock.unlock()

  def session: Session

  def tableName: String

  def initialize(): Unit

  def getKey(row: Row): Any

  def getValue(row: Row): Any

  def convertValue(value: Any): Object

  def get(key: Any): Any = {
    val stmt = new BoundStatement(getStmt)
    val results = session.execute(stmt.bind(key.asInstanceOf[Object]))
    getValue(results.one())
  }

  def put(key: Any, value: Any) {
    val stmt = new BoundStatement(putStmt)

    session.execute(stmt.bind(
      key.asInstanceOf[Object], convertValue(value)))
  }

  def delete(key: Any) = {
    val stmt = new BoundStatement(deleteStmt)
    session.execute(stmt.bind(key.asInstanceOf[Object]))
  }

  def contains(key: Any): Boolean = {
    val stmt = new BoundStatement(containsStmt)
    val results = session.execute(stmt.bind(key.asInstanceOf[Object]))
    results.one().getLong("count") > 0
  }

  def count(): Int = {
    val stmt = s"""SELECT COUNT(*) FROM $tableName"""
    val results = session.execute(stmt)
    results.one().getLong("count").toInt
  }

  def foreach(process: (Any, Any) => Unit) {
    val stmt = s"""SELECT * FROM $tableName"""
    val results = session.execute(stmt)
    for (row <- results.iterator()) {
      process(getKey(row), getValue(row))
    }
  }

  def processKeys(process: Iterable[Any] => Unit) = {
    val stmt = s"""SELECT * FROM $tableName"""
    val results = session.execute(stmt)
    process(results.all().map(getKey(_)))
  }

  def close() {}
}

class CassandraStringDoubleTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange,
     recovery: Boolean) extends CassandraTable {
  def initialize() {
    if (!recovery) {
      session.execute(s"""DROP TABLE IF EXISTS $tableName""")
      val query = s"""CREATE TABLE $tableName
        (key text, value text, PRIMARY KEY(key));"""
      session.execute(query)
    }
  }

  def getKey(row: Row) = row.getString("key")

  def getValue(row: Row) = row.getString("value").toDouble

  def convertValue(value: Any) = value.toString
}

class CassandraStringIntTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange,
     recovery: Boolean) extends CassandraTable {
  def initialize() {
    if (!recovery) {
      session.execute(s"""DROP TABLE IF EXISTS $tableName""")
      val query = s"""CREATE TABLE $tableName
        (key text, value text, PRIMARY KEY(key));"""
      session.execute(query)
    }
  }

  def getKey(row: Row) = row.getString("key")

  def getValue(row: Row) = row.getString("value").toInt

  def convertValue(value: Any) = value.toString
}

class CassandraStringStringTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange,
     recovery: Boolean) extends CassandraTable {
  def initialize() {
    if (!recovery) {
      session.execute(s"""DROP TABLE IF EXISTS $tableName""")
      val query = s"""CREATE TABLE $tableName
        (key text, value text, PRIMARY KEY(key));"""
      session.execute(query)
    }
  }

  def getKey(row: Row) = row.getString("key")

  def getValue(row: Row) = row.getString("value")

  def convertValue(value: Any) = value.asInstanceOf[Object]
}

class CassandraStringLongTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange,
     recovery: Boolean) extends CassandraTable {
  def initialize() {
    if (!recovery) {
      //session.execute(s"""DROP TABLE IF EXISTS $tableName""")
      /*val query = s"""CREATE TABLE IF NOT EXISTS $tableName
        (key text, value text, PRIMARY KEY(key));"""
      session.execute(query)*/
    }
  }

  def getKey(row: Row) = row.getString("key")

  def getValue(row: Row) = row.getString("value").toLong

  def convertValue(value: Any) = value.toString
}

class CassandraModTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange) extends CassandraTable {
  def initialize() {}

  def getKey(row: Row) = row.getLong("key")

  def getValue(row: Row) = {
    val bytes = row.getBytes("value")
    val hexString = Bytes.toHexString(bytes)
    val byteBuffer = Bytes.fromHexString(hexString)
    Util.deserialize(byteBuffer.array())
  }

  def convertValue(value: Any) = {
    val bytes = Util.serialize(value)
    val hexString = Bytes.toHexString(bytes)
    Bytes.fromHexString(hexString)
  }
}

class CassandraIntAnyTable
    (val session: Session,
     val tableName: String,
     val hashRange: HashRange) extends CassandraTable {
  def initialize() {}

  def getKey(row: Row) = row.getLong("key")

  def getValue(row: Row) = {
    val bytes = row.getBytes("value")
    val hexString = Bytes.toHexString(bytes)
    val byteBuffer = Bytes.fromHexString(hexString)
    Util.deserialize(byteBuffer.array())
  }

  def convertValue(value: Any) = {
    val bytes = Util.serialize(value)
    val hexString = Bytes.toHexString(bytes)
    Bytes.fromHexString(hexString)
  }
}
