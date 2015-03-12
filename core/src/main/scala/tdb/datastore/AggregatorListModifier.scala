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

  //println("new AggregatorListModifier")

  private val values = Map[Any, Mod[U]]()

  def loadInput(keys: Iterator[Any]) = ???

  def put(key: Any, anyValue: Any): Future[_] = {
    val value = anyValue.asInstanceOf[U]

    if (values.contains(key)) {
      val oldValue = datastore.read(values(key))

      val newValue = conf.aggregator(oldValue, value)

      datastore.updateMod(values(key).id, newValue)
    } else {
      val mod = datastore.createMod(value)

      values(key) = mod

      Future { "done" }
    }
  }

  def putIn(column: String, key: Any, value: Any): Future[_] = ???

  def get(key: Any): Any = {
    val mod = values(key)
    datastore.read(mod)
  }

  def remove(key: Any, anyValue: Any): Future[_] = {
    val value = anyValue.asInstanceOf[U]

    val mod = values(key)
    val oldValue = datastore.read(mod)

    var newValue = conf.deaggregator(value, oldValue)

    if (newValue == 0) {
      values -= key
      datastore.removeMods(Iterable(mod.id), null)
    } else {
      datastore.updateMod(mod.id, newValue)
    }
  }

  def contains(key: Any): Boolean = {
    values.contains(key)
  }

  def getAdjustableList() = new AggregatorList(listId, datastoreRef)

  def toBuffer(): Buffer[(Any, Any)] = {
    val buf = Buffer[(Any, Any)]()

    for ((key, mod) <- values) {
      buf += ((key, datastore.read(mod)))
    }

    buf
  }
}
