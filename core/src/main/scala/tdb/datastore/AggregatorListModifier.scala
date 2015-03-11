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

import akka.actor.ActorRef
import scala.collection.mutable.{Buffer, Map, Set}
import scala.concurrent.{ExecutionContext, Future}

import tdb.{Mod, Mutator}
import tdb.Constants._
import tdb.list._

class AggregatorListModifier[U]
    (listId: String,
     datastore: Datastore,
     datastoreRef: ActorRef,
     conf: AggregatorListConf[U])
    (implicit ec: ExecutionContext)
  extends Modifier {

  private val values = Map[Any, Mod[Vector[(Any, U)]]]()

  private val fullChunks = Set[ModId]()
  private val freeChunks = Set[ModId]()

  def loadInput(keys: Iterator[Any]) = ???

  def put(key: Any, anyValue: Any): Future[_] = {
    val value = anyValue.asInstanceOf[U]

    if (values.contains(key)) {
      val chunk = datastore.read(values(key))

      val newChunk = chunk.map {
        case (_key, _value) => {
          if (key == _key) {
            (_key, conf.aggregator(_value, value))
          } else {
            (_key, _value)
          }
        }
      }

      datastore.updateMod(values(key).id, newChunk)
    } else {
      if (freeChunks.size > 0) {
        val modId = freeChunks.head
        val chunk = datastore.readId[Vector[(Any, Any)]](modId)

        if (chunk.size + 1 > conf.chunkSize) {
          fullChunks += modId
          freeChunks -= modId
        }

        values(key) = values(chunk.head._1)

        val newChunk = chunk :+ (key, value)

        datastore.updateMod(modId, newChunk)
      } else {
        val chunk = Vector((key, value))

        val mod = datastore.createMod(chunk)
        values(key) = mod
        freeChunks += mod.id

        Future { "done" }
      }
    }
  }

  def putIn(column: String, key: Any, value: Any): Future[_] = ???

  def get(key: Any): Any = {
    val mod = values(key)
    var value: Any = null
    datastore.read(mod).foreach {
      case (k, v) => if (k == key) value = v
    }
    value
  }

  def remove(key: Any, anyValue: Any): Future[_] = {
    val value = anyValue.asInstanceOf[U]

    val mod = values(key)
    val chunk = datastore.read(mod)

    var newValue: Any = null
    var newChunk = chunk.map{ case (_key, _value) => {
      if (key == _key) {
        assert(newValue == null)
        newValue = conf.deaggregator(_value, value)
        (_key, newValue)
      } else {
        (_key, _value)
      }
    }}.filter(_._2 != 0)
    assert(newValue != null)

    if (newValue == 0) {
      values -= key

      if (fullChunks.contains(mod.id)) {
        fullChunks -= mod.id
      }

      if (newChunk.size > 0) {
        freeChunks += mod.id
      }
    }

    if (newChunk.size > 0) {
      datastore.updateMod(mod.id, newChunk)
    } else {
      freeChunks -= mod.id
      datastore.removeMods(Iterable(mod.id), null)
      Future { "done" }
    }
  }

  def contains(key: Any): Boolean = {
    values.contains(key)
  }

  def getAdjustableList() = new AggregatorList(listId, datastoreRef)

  def toBuffer(): Buffer[(Any, Any)] = {
    val buf = Buffer[(Any, Any)]()
    for (modId <- fullChunks) {
      buf ++= datastore.readId[Vector[(Any, Any)]](modId)
    }

    for (modId <- freeChunks) {
      buf ++= datastore.readId[Vector[(Any, Any)]](modId)
    }

    buf
  }
}
