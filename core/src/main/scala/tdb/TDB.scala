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
package tdb

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.Map
import scala.concurrent.Await

import tdb.Constants._
import tdb.list._
import tdb.messages._
import tdb.TDB._

object TDB {
  def createList[T, U](conf: ListConf)
      (implicit c: Context): ListInput[T, U] = {
    val future = c.masterRef ? CreateListMessage(conf)
    val input = Await.result(future.mapTo[ListInput[T, U]], DURATION)
    input
  }

  def put[T, U](input: ListInput[T, U], key: T, value: U)
      (implicit c: Context) {
    val timestamp =
      c.ddg.addPut(input.asInstanceOf[ListInput[Any, Any]], key, value, c)

    val anyInput = input.asInstanceOf[HashPartitionedListInput[Any, Any]]
    if (!c.buffers.contains(anyInput)) {
      c.buffers(anyInput) = anyInput.getBuffer()
    }
    c.buffers(anyInput).putAll(Iterable((key, value)))

    timestamp.end = c.ddg.nextTimestamp(timestamp.node, c)
  }

  def putAll[T, U](input: ListInput[T, U], values: Iterable[(T, U)])
      (implicit c: Context) {
    val timestamp =
      c.ddg.addPutAll(input.asInstanceOf[ListInput[Any, Any]], values, c)

    val anyInput = input.asInstanceOf[HashPartitionedListInput[Any, Any]]
    if (!c.buffers.contains(anyInput)) {
      c.buffers(anyInput) = anyInput.getBuffer()
    }
    c.buffers(anyInput).putAll(values)

    timestamp.end = c.ddg.nextTimestamp(timestamp.node, c)
  }

  def get[T, U](input: ListInput[T, U], key: T)
      (getter: U => Unit)
      (implicit c: Context): Unit = {
    val timestamp =
      c.ddg.addGet(
        input.asInstanceOf[ListInput[Any, Any]],
        key,
        getter.asInstanceOf[Any => Unit],
        c)

    val value = input.get(key, c.task.self)
    getter(value)

    timestamp.node.currentModId = c.currentModId
    timestamp.end = c.ddg.nextTimestamp(timestamp.node, c)
  }

  def read[T, U](mod: Mod[T])
      (reader: T => Changeable[U])
      (implicit c: Context): Changeable[U] = {
    val value = c.read(mod, c.task.self)

    val timestamp = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c)
    val readNode = timestamp.node

    val changeable = reader(value)

    readNode.currentModId = c.currentModId
    timestamp.end = c.ddg.nextTimestamp(readNode, c)

    changeable
  }

  def readAny[T](mod: Mod[T])
      (reader: T => Unit)
      (implicit c: Context) {
    val value = c.read(mod, c.task.self)

    val timestamp = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c)
    val readNode = timestamp.node

    val ret = reader(value)

    readNode.currentModId = c.currentModId
    timestamp.end = c.ddg.nextTimestamp(readNode, c)
  }

  def read2[T, U, V](mod: Mod[T])
      (reader: T => (Changeable[U], Changeable[V]))
      (implicit c: Context): (Changeable[U], Changeable[V]) = {
    val value = c.read(mod, c.task.self)

    val timestamp = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c)
    val readNode = timestamp.node

    val changeables = reader(value)

    readNode.currentModId = c.currentModId
    readNode.currentModId2 = c.currentModId2
    timestamp.end = c.ddg.nextTimestamp(readNode, c)

    changeables
  }

  def read_2[T, U, V](mod1: Mod[T], mod2: Mod[U])
      (reader: (T, U) => Changeable[V])
      (implicit c: Context): Changeable[V] = {
    val value1 = c.read(mod1, c.task.self)
    val value2 = c.read(mod2, c.task.self)

    val timestamp = c.ddg.addRead2(
      mod1.asInstanceOf[Mod[Any]],
      mod2.asInstanceOf[Mod[Any]],
      value1,
      value2,
      reader.asInstanceOf[(Any, Any) => Changeable[Any]],
      c)
    val read2Node = timestamp.node

    val changeable = reader(value1, value2)

    read2Node.currentModId = c.currentModId
    timestamp.end = c.ddg.nextTimestamp(read2Node, c)

    changeable
  }

  def mod[T](initializer: => Changeable[T])
     (implicit c: Context): Mod[T] = {
    val mod1 = new Mod[T](c.newModId())

    modInternal(initializer, mod1, null, null, c)
  }

  def modInternal[T]
      (initializer: => Changeable[T],
       mod1: Mod[T],
       modizer: Modizer[T],
       key: Any,
       c: Context): Mod[T] = {
    val oldCurrentModId = c.currentModId

    c.currentModId = mod1.id

    val timestamp = c.ddg.addMod(
      mod1.id,
      -1,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)
    val modNode = timestamp.node

    initializer

    timestamp.end = c.ddg.nextTimestamp(modNode, c)

    c.currentModId = oldCurrentModId

    mod1
  }

  def mod2[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) = {
    val mod1 = new Mod[T](c.newModId())
    val mod2 = new Mod[U](c.newModId())

    mod2Internal(initializer, mod1, mod2, null, null, c)
  }

  def mod2Internal[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       modLeft: Mod[T],
       modRight: Mod[U],
       modizer: Modizer2[T, U],
       key: Any,
       c: Context): (Mod[T], Mod[U]) = {
    val oldCurrentModId = c.currentModId
    c.currentModId = modLeft.id

    val oldCurrentModId2 = c.currentModId2
    c.currentModId2 = modRight.id

    val timestamp = c.ddg.addMod(
      modLeft.id,
      modRight.id,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)
    val modNode = timestamp.node

    initializer

    timestamp.end = c.ddg.nextTimestamp(modNode, c)

    c.currentModId = oldCurrentModId
    c.currentModId2 = oldCurrentModId2

    (modLeft, modRight)
  }

  def modLeft[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) = {
    modLeftInternal(initializer, new Mod[T](c.newModId()), null, null, c)
  }

  def modLeftInternal[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       modLeft: Mod[T],
       modizer: Modizer2[T, U],
       key: Any,
       c: Context): (Mod[T], Changeable[U]) = {

    val oldCurrentModId = c.currentModId
    c.currentModId = modLeft.id

    val timestamp = c.ddg.addMod(
      modLeft.id,
      -1,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)
    val modNode = timestamp.node

    modNode.currentModId2 = c.currentModId2

    initializer

    timestamp.end = c.ddg.nextTimestamp(modNode, c)

    c.currentModId = oldCurrentModId

    (modLeft, new Changeable[U](c.currentModId2))
  }

  def modRight[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) = {
    modRightInternal(initializer, new Mod[U](c.newModId()), null, null, c)
  }

  def modRightInternal[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       modRight: Mod[U],
       modizer: Modizer2[T, U],
       key: Any,
       c: Context): (Changeable[T], Mod[U]) = {
    val oldCurrentModId2 = c.currentModId2
    c.currentModId2 = modRight.id

    val timestamp = c.ddg.addMod(
      -1,
      modRight.id,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)
    val modNode = timestamp.node

    modNode.currentModId = c.currentModId

    initializer

    timestamp.end = c.ddg.nextTimestamp(modNode, c)

    c.currentModId2 = oldCurrentModId2

    (new Changeable[T](c.currentModId), modRight)
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    c.update(c.currentModId, value)

    (new Changeable[T](c.currentModId))
  }

  def write2[T, U](value: T, value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    c.update(c.currentModId, value)

    c.update(c.currentModId2, value2)

    (new Changeable[T](c.currentModId),
     new Changeable[U](c.currentModId2))
  }

  def writeLeft[T, U](value: T, changeable: Changeable[U])
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    c.update(c.currentModId, value)

    (new Changeable[T](c.currentModId),
     new Changeable[U](c.currentModId2))
  }

  def writeRight[T, U](changeable: Changeable[T], value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    c.update(c.currentModId2, value2)

    (new Changeable[T](c.currentModId),
     new Changeable[U](c.currentModId2))
  }

  def parWithHint[T, U](one: Context => T, workerId1: WorkerId = -1)
      (two: Context => U, workerId2: WorkerId = -1)
      (implicit c: Context): (T, U) = {
    val future1 = c.masterRef ? ScheduleTaskMessage(c.task.self, workerId1)
    val taskRef1 = Await.result(future1.mapTo[ActorRef], DURATION)

    val adjust1 = new Adjustable[T] { def run(implicit c: Context) = one(c) }
    val oneFuture = taskRef1 ? RunTaskMessage(adjust1)

    val future2 = c.masterRef ? ScheduleTaskMessage(c.task.self, workerId2)
    val taskRef2 = Await.result(future2.mapTo[ActorRef], DURATION)

    val adjust2 = new Adjustable[U] { def run(implicit c: Context) = two(c) }
    val twoFuture = taskRef2 ? RunTaskMessage(adjust2)

    val parNode = c.ddg.addPar(taskRef1, taskRef2, c)

    val oneRet = Await.result(oneFuture, DURATION).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, DURATION).asInstanceOf[U]
    (oneRet, twoRet)
  }

  def par[T](one: Context => T): Parizer[T] = {
    new Parizer(one)
  }
}
