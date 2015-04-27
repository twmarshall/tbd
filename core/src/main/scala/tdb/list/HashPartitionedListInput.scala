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
package tdb.list

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.{Buffer, Map}
import scala.concurrent.{Await, Future}

import tdb.Resolver
import tdb.Constants._
import tdb.messages._
import tdb.util.ObjHasher

abstract class HashPartitionedListInput[T, U](masterRef: ActorRef)
  extends ListInput[T, U] with java.io.Serializable {

  import scala.concurrent.ExecutionContext.Implicits.global

  protected val resolver = new Resolver(masterRef)

  def hasher: ObjHasher[TaskId]

  def workers: Iterable[ActorRef]

  def conf: ListConf

  def loadFile(fileName: String) = {
    val dir = fileName + "-split" + conf.partitions
    val workerFutures = workers.map {
      case workerRef =>
        workerRef ? SplitFileMessage(dir, fileName, conf.partitions)
    }

    import scala.concurrent.ExecutionContext.Implicits.global
    Await.result(Future.sequence(workerFutures), DURATION)

    val futures = hasher.objs.map {
      case (hash, datastoreId) =>
      val thisFile = dir + "/" + hash
      resolver.send(datastoreId, LoadFileMessage(thisFile))
    }
    Await.result(Future.sequence(futures), DURATION)
  }

  def put(key: T, value: U) = {
    Await.result(asyncPut(key, value), DURATION)
  }

  def asyncPut(key: T, value: U) = {
    val datastoreId = hasher.getObj(key)
    resolver.send(datastoreId, PutMessage("keys", key, value, null))
  }

  def get(key: T, taskRef: ActorRef): U = {
    val datastoreId = hasher.getObj(key)
    val f = resolver.send(datastoreId, GetMessage(key, taskRef))
    Await.result(f, DURATION).asInstanceOf[U]
  }

  def remove(key: T, value: U) = {
    Await.result(asyncRemove(key, value), DURATION)
  }

  def removeAll(values: Iterable[(T, U)]) {
    val futures = Buffer[Future[Any]]()
    val hashedRemove = hasher.hashAll(values)

    for ((hash, buf) <- hashedRemove) {
      if (buf.size > 0) {
        val datastoreId = hasher.objs(hash)
        futures += resolver.send(datastoreId, RemoveAllMessage(buf))
      }
    }

    Await.result(Future.sequence(futures), DURATION)
  }

  def asyncRemove(key: T, value: U): Future[_] = {
    val datastoreId = hasher.getObj(key)
    resolver.send(datastoreId, RemoveMessage(key, value))
  }

  def load(data: Map[T, U]) = {
    for ((key, value) <- data) {
      put(key, value)
    }
  }

  def getBuffer(): InputBuffer[T, U] = new HashBuffer(this)
}

class HashBuffer[T, U](input: HashPartitionedListInput[T, U]) extends InputBuffer[T, U] {
  import scala.concurrent.ExecutionContext.Implicits.global

  private val toPut = Map[T, U]()

  private val toRemove = Map[T, U]()

  def putAll(values: Iterable[(T, U)]) {
    for ((key, value) <- values) {
      if (toRemove.contains(key)) {
        toRemove -= key
      } else {
        toPut += ((key, value))
      }
    }
  }

  def putAllIn(column: String, values: Iterable[(T, Any)]) = ???

  def removeAll(values: Iterable[(T, U)]) {
    for ((key, value) <- values) {
      if (toPut.contains(key)) {
        // This is only called by Ordering.splice, so if we're removing a value
        // that was already put we can just cancel those out.
        if (toPut(key) == value) {
          toPut -= key
        }
      } else {
        toRemove += ((key, value))
      }
    }
  }

  private def asyncPutAll
      (values: Iterable[(T, U)], resolver: Resolver): Future[_] = {
    val hashedPut = input.hasher.hashAll(values)

    val futures = Buffer[Future[Any]]()
    for ((hash, buf) <- hashedPut) {
      if (buf.size > 0) {
        val datastoreId = input.hasher.objs(hash)
        futures += resolver.send(datastoreId, PutAllMessage(buf))
      }
    }

    Future.sequence(futures)
  }

  def flush(resolver: Resolver, recovery: Boolean) {
    val futures = Buffer[Future[Any]]()

    futures += asyncPutAll(toPut, resolver)
    input.removeAll(toRemove)

    toRemove.clear()
    toPut.clear()

    Await.result(Future.sequence(futures), DURATION)
  }
}
