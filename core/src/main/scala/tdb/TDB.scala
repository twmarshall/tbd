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
import scala.collection.immutable
import scala.collection.mutable.Map
import scala.concurrent.Await

import tdb.Constants._
import tdb.ddg._
import tdb.list._
import tdb.messages._
import tdb.TDB._

object TDB {
  def createList[T, U](conf: ListConf)
      (implicit c: Context): ListInput[T, U] = {
    val future = c.masterRef ? CreateListMessage(conf, c.taskId)
    val input = Await.result(future.mapTo[ListInput[T, U]], DURATION)
    input
  }

  def getAdjustableList[T, U](input: ListInput[T, U])
      (implicit c: Context): AdjustableList[T, U] = {
    input.getList(c.resolver)
  }

  def put[T, U](input: ListInput[T, U], key: T, value: U)
      (implicit c: Context) {
    val anyInput = input.asInstanceOf[ListInput[Any, Any]]
    val timestamp = c.ddg.addPut(anyInput, key, value, c)

    if (!c.buffers.contains(anyInput)) {
      c.buffers(anyInput) = anyInput.getBuffer()
    }
    c.buffers(anyInput).putAll(Iterable((key, value)))

    timestamp.end = c.ddg.nextTimestamp(timestamp.node, c)
  }

  def putAll[T, U](input: ListInput[T, U], values: Iterable[(T, U)])
      (implicit c: Context) {
    val anyInput = input.asInstanceOf[ListInput[Any, Any]]
    val timestamp = c.ddg.addPutAll(anyInput, values, c)

    if (!c.buffers.contains(anyInput)) {
      c.buffers(anyInput) = anyInput.getBuffer()
    }
    c.buffers(anyInput).putAll(values)

    timestamp.end = c.ddg.nextTimestamp(timestamp.node, c)
  }


  def putIn[T]
      (traceable: Traceable[T, _, _], parameters: T)
      (implicit c: Context) {
    val anyTraceable = traceable.asInstanceOf[Traceable[Any, Any, Any]]
    val timestamp = c.ddg.addPutIn(
      anyTraceable, parameters.asInstanceOf[Any], c)

    if (!c.bufs.contains(traceable.inputId)) {
      c.bufs(traceable.inputId) = anyTraceable.getTraceableBuffer()
    }
    c.bufs(traceable.inputId).putIn(parameters)

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

    val value = input.get(key, c.taskRef)
    getter(value)

    timestamp.node.currentModId = c.currentModId
    timestamp.end = c.ddg.nextTimestamp(timestamp.node, c)
  }

  def getFrom[T, U]
      (traceable: Traceable[_, T, U], parameters: T)
      (getter: U => Unit)
      (implicit c: Context) {
    val getNode = new GetFromNode(
      traceable.asInstanceOf[Traceable[Any, Any, Any]],
      parameters.asInstanceOf[Any],
      getter.asInstanceOf[Any => Unit])
    val timestamp = c.ddg.nextTimestamp(getNode, c)
    c.ddg.nodes(c.nextNodeId) = timestamp

    val value = traceable.get(parameters, c.nextNodeId, c.taskRef)
    getter(value)

    c.nextNodeId += 1
    timestamp.end = c.ddg.nextTimestamp(timestamp.node, c)
  }

  def read[T, U](mod: Mod[T])
      (reader: T => Changeable[U])
      (implicit c: Context): Changeable[U] = {
    val value = c.read(mod, c.taskRef)

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
    val value = c.read(mod, c.taskRef)

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
    val value = c.read(mod, c.taskRef)

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

  def read_2[T, U, V, W](mod1: Mod[T], mod2: Mod[U])
      (reader: (T, U) => W)
      (implicit c: Context): W = {
    val value1 = c.read(mod1, c.taskRef)
    val value2 = c.read(mod2, c.taskRef)

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

  def read_3[T, U, V, W](mod1: Mod[T], mod2: Mod[U], mod3: Mod[V])
      (reader: (T, U, V) => W)
      (implicit c: Context): W = {
    val value1 = c.read(mod1, c.taskRef)
    val value2 = c.read(mod2, c.taskRef)
    val value3 = c.read(mod3, c.taskRef)

    val timestamp = c.ddg.addRead3(
      mod1.asInstanceOf[Mod[Any]],
      mod2.asInstanceOf[Mod[Any]],
      mod3.asInstanceOf[Mod[Any]],
      value1,
      value2,
      value3,
      reader.asInstanceOf[(Any, Any, Any) => Changeable[Any]],
      c)
    val read3Node = timestamp.node

    val changeable = reader(value1, value2, value3)

    read3Node.currentModId = c.currentModId
    timestamp.end = c.ddg.nextTimestamp(read3Node, c)

    changeable
  }

  def mod[T](initializer: => Changeable[T])
     (implicit c: Context): Mod[T] = {
    val mod1 = new Mod[T](c.newModId())

    modInternal(initializer, mod1, c)
  }

  def modInternal[T]
      (initializer: => Changeable[T],
       mod1: Mod[T],
       c: Context): Mod[T] = {
    val oldCurrentModId = c.currentModId

    c.currentModId = mod1.id

    val timestamp = c.ddg.addMod(
      mod1.id,
      -1,
      c)
    val modNode = timestamp.node

    initializer

    timestamp.end = c.ddg.nextTimestamp(modNode, c)

    c.currentModId = oldCurrentModId

    mod1
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

  def parWithHint[T, U]
      (one: Context => T, datastoreId1: TaskId = -1, name1: String = "")
      (two: Context => U, datastoreId2: TaskId = -1, name2: String = "")
      (implicit c: Context): (T, U) = {
    innerPar(one, datastoreId1, name1)(two, datastoreId2, name2)(c)
  }

  def innerPar[T, U]
      (one: Context => T, datastoreId1: TaskId = -1, name1: String = "")
      (two: Context => U, datastoreId2: TaskId = -1, name2: String = "")
      (implicit c: Context): (T, U) = {
    val adjust1 = new Adjustable[T] {
      def run(implicit c: Context) = {
        one(c)
      }
    }

    val future1 = c.masterRef ? ScheduleTaskMessage(
      name1, c.taskId, datastoreId1, adjust1)

    val adjust2 = new Adjustable[U] {
      def run(implicit c: Context) = {
        two(c)
      }
    }

    val future2 = c.masterRef ? ScheduleTaskMessage(
      name2, c.taskId, datastoreId2, adjust2)

    val (taskId1, oneRet) = Await.result(
      future1.mapTo[(TaskId, T)], DURATION)
    val (taskId2, twoRet) = Await.result(
      future2.mapTo[(TaskId, U)], DURATION)

    val parNode = c.ddg.addPar(taskId1, taskId2, c)

    (oneRet, twoRet)
  }

  def par[T](one: Context => T): Parizer[T] = {
    new Parizer(one)
  }
}
