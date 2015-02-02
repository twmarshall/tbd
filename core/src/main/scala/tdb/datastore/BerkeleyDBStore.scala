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

import akka.actor.{ActorRef, ActorContext, Props}
import akka.pattern.{ask, pipe}
import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, PrimaryIndex, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey, SecondaryKey}
import com.sleepycat.persist.model.Relationship
import java.io._
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.Future
import scala.util.{Failure, Success}

import tdb.messages._
import tdb.Mod
import tdb.Constants.ModId

class LRUNode(
  val key: ModId,
  var value: Any,
  var previous: LRUNode,
  var next: LRUNode
)

class BerkeleyDBStore(maxCacheSize: Int) extends Datastore {
  import context.dispatcher

  private var environment: Environment = null
  private var modStore: EntityStore = null
  private var pIdx: PrimaryIndex[java.lang.Long, ModEntity] = null

  private var inputStore: EntityStore = null
  private var inputId: PrimaryIndex[String, InputEntity] = null

  private val envConfig = new EnvironmentConfig()
  envConfig.setCacheSize(96 * 1024)
  envConfig.setAllowCreate(true)

  val modStoreConfig = new StoreConfig()
  modStoreConfig.setAllowCreate(true)

  val random = new scala.util.Random()
  private var envHome: File = null

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

  init()

  private def init() {
    envHome = new File("/tmp/tdb_berkeleydb")
    envHome.mkdir()

    try {
      // Open the environment and entity store
      environment = new Environment(envHome, envConfig)
      modStore = new EntityStore(environment, "ModStore", modStoreConfig)
    } catch {
      case fnfe: FileNotFoundException => {
        System.err.println("setup(): " + fnfe.toString())
        System.exit(-1)
      }
    }

    pIdx = modStore.getPrimaryIndex(classOf[java.lang.Long], classOf[ModEntity])
  }

  def put(key: ModId, value: Any) {
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

        putInDB(key, value)

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
          putInDB(key, value)
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
  }

  private def putInDB(key: ModId, value: Any) {
    writeCount += 1

    val entity = new ModEntity()
    entity.key = key
    val byteOutput = new ByteArrayOutputStream()
    val objectOutput = new ObjectOutputStream(byteOutput)
    objectOutput.writeObject(value)
    entity.value = byteOutput.toByteArray

    pIdx.put(entity)
  }

  def get(key: ModId): Any = {
    if (values.contains(key)) {
      values(key).value
    } else {
      getFromDB(key)
    }
  }

  def asyncGet(key: ModId): Future[Any] = {
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
        val value = getFromDB(key)
        if (value == null) {
          NullMessage
        } else {
          value
        }
      }
    }
  }

  private def getFromDB(key: ModId): Any = {
    readCount += 1
    val byteArray = pIdx.get(key).value

    val byteInput = new ByteArrayInputStream(byteArray)
    val objectInput = new ObjectInputStream(byteInput)
    val obj = objectInput.readObject()

    // No idea why this is necessary.
    if (obj != null && obj.isInstanceOf[Tuple2[_, _]]) {
      obj.toString
    }

    obj
  }

  def remove(key: ModId) {
    if (values.contains(key)) {
      values -= key
    } else {
      deleteCount += 1
      pIdx.delete(key)
    }
  }

  def contains(key: ModId) = {
    values.contains(key) || pIdx.contains(key)
  }

  def clear() = {
    values.clear()

    cacheSize = 0
    head = tail
    tail.previous = null
    tail.next = null

    modStore.close()
    if (inputStore != null) {
      inputStore.close()
    }
    environment.close()
    init()
  }

  def shutdown() {
    println("Shutting down. writes = " + writeCount + ", reads = " +
            readCount + ", deletes = " + deleteCount)
    modStore.close()
    environment.close()
  }

  def putInput(key: String, value: String) {
    val entity = new InputEntity()
    entity.key = key
    entity.value = value
    inputId.put(entity)
  }

  def retrieveInput(inputName: String): Boolean = {
    inputStore = new EntityStore(environment, inputName, modStoreConfig)
    inputId = inputStore.getPrimaryIndex(classOf[String], classOf[InputEntity])

    println(inputId.count())
    inputId.count() > 0
  }


  def iterateInput(process: Iterable[String] => Unit, partitions: Int) {
    val cursor = inputId.keys()
    val partitionSize = inputId.count() / partitions + 1

    val buf = Buffer[String]()
    val it = cursor.iterator
    while (it.hasNext) {
      if (buf.size < partitionSize) {
        buf += it.next()
      } else {
        process(buf)
        buf.clear()
      }
    }

    if (buf.size > 0) {
      process(buf)
    }

    cursor.close()
  }

  def getInput(key: String) = {
    Future {
      (key, inputId.get(key).value)
    }
  }
}

@Entity
class ModEntity {
  @PrimaryKey
  var key: java.lang.Long = -1

  var value: Array[Byte] = null
}

@Entity
class InputEntity {
  @PrimaryKey
  var key: String = ""

  var value: String = ""
}
