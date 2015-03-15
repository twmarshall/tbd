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
import scala.concurrent.{Await, ExecutionContext, Future}

import tdb.{Mod, Mutator}
import tdb.Constants._
import tdb.list._

class AggregatorListModifier[U]
    (datastore: Datastore,
     datastoreRef: ActorRef,
     conf: AggregatorListConf[U])
    (implicit ec: ExecutionContext)
  extends Modifier {

  //println("new AggregatorListModifier")

  private val dummyFuture = Future { "done" }

  private val values = Map[Any, Any]()

  def loadInput(keys: Iterable[Any]) = ???

  def put(key: Any, anyValue: Any): Future[_] = {
    val value = anyValue.asInstanceOf[U]

    if (values.contains(key)) {
      val oldValue = values(key).asInstanceOf[U]
      val newValue = conf.aggregator(oldValue, value)
      values(key) = newValue
    } else {
      values(key) = value
    }

    dummyFuture
  }

  def putIn(column: String, key: Any, value: Any): Future[_] = ???

  def get(key: Any): Any = {
    values(key)
  }

  def remove(key: Any, anyValue: Any): Future[_] = {
    val value = anyValue.asInstanceOf[U]

    val oldValue = values(key).asInstanceOf[U]

    var newValue = conf.deaggregator(value, oldValue)

    if (newValue == 0) {
      values -= key
    } else {
      values(key) = newValue
    }

    dummyFuture
  }

  def contains(key: Any): Boolean = {
    values.contains(key)
  }

  def getAdjustableList() = new AggregatorList(datastoreRef)

  def toBuffer(): Buffer[(Any, Any)] = {
    val buf = Buffer[(Any, Any)]()

    for ((key, value) <- values) {
      buf += ((key, value))
    }

    buf
  }
}
