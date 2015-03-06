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

class AggregatorListModifier
    (listId: String,
     datastore: Datastore,
     datastoreRef: ActorRef,
     conf: ListConf)
    (implicit ec: ExecutionContext)
  extends Modifier {

  private val values = Map[Any, Mod[Vector[(Any, Int)]]]()

  private val fullChunks = Set[ModId]()
  private val freeChunks = Set[ModId]()

  def loadInput(keys: Iterator[Any]) = ???

  def put(key: Any, value: Any): Future[_] = {
    if (values.contains(key)) {
      val chunk = datastore.read(values(key))

      val newChunk = chunk.map {
        case (_key: Any, _value: Int) => {
          if (key == _key) {
            (_key, _value + value.asInstanceOf[Int])
          } else {
            (_key, _value)
          }
        }
      }

      datastore.updateMod(values(key).id, newChunk)
    } else {
      if (freeChunks.size > 0) {
        val modId = freeChunks.head
        val chunk = datastore.readId[Vector[(Any, Int)]](modId)

        if (chunk.size + 1 > conf.chunkSize) {
          fullChunks += modId
          freeChunks -= modId
        }

        values(key) = values(chunk.head._1)

        val newChunk = chunk :+ (key, value.asInstanceOf[Int])

        datastore.updateMod(modId, newChunk)
      } else {
        val chunk = Vector((key, value.asInstanceOf[Int]))

        val mod = datastore.createMod(chunk)
        values(key) = mod
        freeChunks += mod.id

        Future { "done" }
      }
    }
  }

  def remove(key: Any, value: Any): Future[_] = {
    val mod = values(key)
    val chunk = datastore.read(mod)

    var newValue = -1
    var newChunk = chunk.map{ case (_key, _value) => {
      if (key == _key) {
        assert(newValue == -1)
        newValue = _value - value.asInstanceOf[Int]
        (_key, newValue)
      } else {
        (_key, _value)
      }
    }}.filter(_._2 != 0)
    assert(newValue >= 0)

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
      buf ++= datastore.readId[Vector[(Any, Int)]](modId)
    }

    for (modId <- freeChunks) {
      buf ++= datastore.readId[Vector[(Any, Int)]](modId)
    }

    buf
  }
}