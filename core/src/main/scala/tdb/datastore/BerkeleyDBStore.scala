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
import akka.pattern.ask
import com.sleepycat.je.{Environment, EnvironmentConfig}
import com.sleepycat.persist.{EntityStore, StoreConfig}
import com.sleepycat.persist.model.{Entity, PrimaryKey, SecondaryKey}
import com.sleepycat.persist.model.Relationship
import java.io._
import scala.collection.mutable.Map
import scala.concurrent.Await

import tdb.Mod
import tdb.Constants.ModId

class LRUNode(
  val key: ModId,
  var value: Any,
  var previous: LRUNode,
  var next: LRUNode
)

class BerkeleyDBStore
    (maxCacheSize: Int,
     context: ActorContext) extends KVStore {
  private var environment: Environment = null
  private var store: EntityStore = null

  private val envConfig = new EnvironmentConfig()
  envConfig.setCachePercent(10)

  envConfig.setAllowCreate(true)
  val storeConfig = new StoreConfig()
  storeConfig.setAllowCreate(true)

  val random = new scala.util.Random()
  private val envHome = new File("/tmp/tdb_berkeleydb" + random.nextInt())
  envHome.mkdir()

  try {
    // Open the environment and entity store
    environment = new Environment(envHome, envConfig)
    store = new EntityStore(environment, "EntityStore", storeConfig)
  } catch {
    case fnfe: FileNotFoundException => {
      System.err.println("setup(): " + fnfe.toString())
      System.exit(-1)
    }
  }

  val pIdx = store.getPrimaryIndex(classOf[java.lang.Long], classOf[ModEntity])

  // LRU cache
  private val values = Map[ModId, LRUNode]()
  private val tail = new LRUNode(-1, null, null, null)
  private var head = tail

  // Statistics
  var readCount = 0
  var writeCount = 0
  var deleteCount = 0

  var cacheSize = 0

  // Calculates size of value, in bytes.
  private def getSize(value: Any): Int = {
    value match {
      case null =>
        1
      case s: String =>
        s.size * 2
      case node: tdb.list.ModListNode[_, _] =>
        getSize(node.value) + getSize(node.nextMod) + 8
      case tuple: Tuple2[_, _] =>
        getSize(tuple._1) + getSize(tuple._2)
      case i: Integer =>
        4
      case m: Mod[_] =>
        getSize(m.id)
      case h: scala.collection.immutable.HashMap[_, _] =>
        h.size * 1000
      case x =>
        println(x.getClass)
        0
    }
  }

  def put(key: ModId, value: Any) {
    /*cacheSize += getSize(value)
    if (values.contains(key)) {
      cacheSize -= getSize(values(key).value)
      values(key).value = value
    } else {
      val newNode = new LRUNode(key, value, null, head)
      values(key) = newNode

      head.previous = newNode
      head = newNode

      while (cacheSize > maxCacheSize * 1024 * 1024) {
        println("evicting")
        val toEvict = tail.previous

        val key = toEvict.key
        val value = toEvict.value

        writeCount += 1
        val entity = new ModEntity()
        entity.key = key

        val byteOutput = new ByteArrayOutputStream()
        val objectOutput = new ObjectOutputStream(byteOutput)
        objectOutput.writeObject(value)
        entity.value = byteOutput.toByteArray

        pIdx.put(entity)
        values -= toEvict.key
        cacheSize -= getSize(value)

        tail.previous = toEvict.previous
        toEvict.previous.next = tail
      }
    }*/

    val entity = new ModEntity()
    entity.key = key

    val byteOutput = new ByteArrayOutputStream()
    val objectOutput = new ObjectOutputStream(byteOutput)
    objectOutput.writeObject(value)
    entity.value = byteOutput.toByteArray

    pIdx.put(entity)
  }

  def get(key: ModId): Any = {
    /*if (values.contains(key)) {
      values(key).value
    } else {
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
    }*/

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
    /*if (values.contains(key)) {
      values -= key
    }*/

    //deleteCount += 1
    pIdx.delete(key)
  }

  def contains(key: ModId) = {
    //values.contains(key) || pIdx.contains(key)
    pIdx.contains(key)
  }

  def clear() = ???

  def shutdown() {
    println("Shutting down. writes = " + writeCount + ", reads = " +
            readCount + ", deletes = " + deleteCount)
    store.close()
    environment.close()
  }
}

@Entity
class ModEntity {
  @PrimaryKey
  var key: java.lang.Long = -1

  var value: Array[Byte] = null
}
