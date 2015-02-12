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

import akka.actor.{ActorRef, ActorContext, Props}
import akka.pattern.{ask, pipe}
import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, PrimaryIndex, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey, SecondaryKey}
import com.sleepycat.persist.model.Relationship
import java.io._
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{ExecutionContext, Future}
import scala.reflect.runtime.universe._
import scala.util.{Failure, Success}

import tdb.datastore.KVStore
import tdb.messages._
import tdb.Mod
import tdb.Constants.ModId
import tdb.util._

class LRUNode(
  val key: ModId,
  var value: Any,
  var previous: LRUNode,
  var next: LRUNode
)

class BerkeleyStore(maxCacheSize: Int)(implicit ec: ExecutionContext) extends KVStore {
  var database = new BerkeleyDatabase()
  private var modStore = database.createModStore()

  private val stores = Map[Int, BerkeleyTable]()

  // LRU cache
  private val values = Map[ModId, LRUNode]()
  private val tail = new LRUNode(-1, null, null, null)
  private var head = tail

  // Statistics
  var readCount = 0
  var writeCount = 0
  var deleteCount = 0

  var cacheSize = 0L

  var n = 0

  private var nextStoreId = 0

  def createTable[T: TypeTag, U: TypeTag]
      (name: String, range: HashRange): Int = {
    val id = nextStoreId
    nextStoreId += 1

    typeOf[T] match {
      case s if typeOf[T] =:= typeOf[String] && typeOf[U] =:= typeOf[String] =>
        stores(id) = database.createInputStore(name, range)
      case m if typeOf[T] =:= typeOf[ModId] =>
        stores(id) = database.createModStore()
      case _ => ???
    }

    id
  }

  def load(id: Int, fileName: String) {
    stores(id).load(fileName)
  }

  def put(id: Int, key: Any, value: Any) = {
    Future {
      stores(id).put(key, value)
    }
  }

  /*def put(key: ModId, value: Any) {
    cacheSize += MemoryUsage.getSize(value)
    if (values.contains(key)) {
      cacheSize -= MemoryUsage.getSize(values(key).value)
      values(key).value = value
    } else {
      val newNode = new LRUNode(key, value, null, head)
      values(key) = newNode

      head.previous = newNode
      head = newNode

      while ((cacheSize / 1024 / 1024) > maxCacheSize) {
        val toEvict = tail.previous

        val key = toEvict.key
        val value = toEvict.value

        modStore.put(key, value)

        values -= toEvict.key
        cacheSize -= MemoryUsage.getSize(value)

        tail.previous = toEvict.previous
        toEvict.previous.next = tail
      }
    }

    if (n % 100000 == 0) {
      println("Current cache size: " + (cacheSize / 1024 / 1024))
    }
    n += 1
  }

  def asyncPut(key: ModId, value: Any): Future[Any] = {
    cacheSize += MemoryUsage.getSize(value)

    val future =
    if (values.contains(key)) {
      cacheSize -= MemoryUsage.getSize(values(key).value)
      values(key).value = value
      Future { "done" }
    } else {
      val newNode = new LRUNode(key, value, null, head)
      values(key) = newNode

      head.previous = newNode
      head = newNode

      val futures = Buffer[Future[String]]()
      while ((cacheSize / 1024 / 1024) > maxCacheSize) {
        val toEvict = tail.previous

        val key = toEvict.key
        val value = toEvict.value

        futures += (Future {
          modStore.put(key, value)
          "done"
        })

        values -= toEvict.key
        cacheSize -= MemoryUsage.getSize(value)

        tail.previous = toEvict.previous
        toEvict.previous.next = tail
      }

      Future.sequence(futures)
    }

    if (n % 100000 == 0) {
      println("Current cache size: " + (cacheSize / 1024 / 1024))
    }
    n += 1

    future
  }*/

  def get(id: Int, key: Any) = {
    Future {
      stores(id).get(key)
    }
  }

  /*def get(key: ModId): Any = {
    if (values.contains(key)) {
      values(key).value
    } else {
      modStore.get(key)
    }
  }*/

  /*def asyncGet(key: ModId): Future[Any] = {
    if (values.contains(key)) {
      val value = values(key).value
      Future {
        if (value == null) {
          NullMessage
        } else {
          value
        }
      }
    } else {
      Future {
        val value = modStore.get(key)
        if (value == null) {
          NullMessage
        } else {
          value
        }
      }
    }
  }*/

  def delete(id: Int, key: Any) {
    stores(id).delete(key)
  }

  /*def remove(key: ModId) {
    if (values.contains(key)) {
      values -= key
    } else {
      deleteCount += 1
      modStore.delete(key)
    }
  }*/

  def contains(id: Int, key: Any): Boolean = {
    stores(id).contains(key)
  }

  def count(id: Int): Int = {
    stores(id).count()
  }

  def clear() = {
    for ((id, store) <- stores) {
      store.close()
    }

    values.clear()

    cacheSize = 0
    head = tail
    tail.previous = null
    tail.next = null

    modStore.close()
    database.close()
    database = new BerkeleyDatabase()
    modStore = database.createModStore()
  }

  def hashedForeach(id: Int)(process: Iterator[Any] => Unit) {
    stores(id).hashedForeach(process)
  }

  def hashRange(id: Int) = {
    stores(id).hashRange
  }

  /*def shutdown() {
    println("Shutting down. writes = " + writeCount + ", reads = " +
            readCount + ", deletes = " + deleteCount)
    modStore.close()
    database.close()
  }*/
}
