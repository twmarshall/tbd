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
package tbd

import akka.actor.ActorRef
import akka.pattern.ask
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

import tbd.Constants._
import tbd.ddg._
import tbd.list.ListInput
import tbd.messages._
import tbd.TBD._

object TBD {
  def put[T, U](input: ListInput[T, U], key: T, value: U)
      (implicit c: Context) {
    val time =
      c.ddg.addPut(input.asInstanceOf[ListInput[Any, Any]], key, value, c)

    val future = input.asyncPut(key, value)
    if (!c.initialRun) {
      c.pending += future
    }

    val end = c.ddg.nextTimestamp(Timestamp.getNodePtr(time), c)
    Timestamp.setEndPtr(time, end)
  }

  def read[T, U](mod: Mod[T])
      (reader: T => Changeable[U])
      (implicit c: Context): Changeable[U] = {
    val value = c.read(mod, c.task.self)

    val time = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c.currentModId,
      -1,
      c)

    val changeable = reader(value)

    val end = c.ddg.nextTimestamp(Timestamp.getNodePtr(time), c)
    Timestamp.setEndPtr(time, end)

    changeable
  }

  def readAny[T](mod: Mod[T])
      (reader: T => Any)
      (implicit c: Context): Any = {
    val value = c.read(mod, c.task.self)

    val time = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c.currentModId,
      -1,
      c)

    val ret = reader(value)

    val end = c.ddg.nextTimestamp(Timestamp.getNodePtr(time), c)
    Timestamp.setEndPtr(time, end)

    ret
  }

  def read2[T, U, V](mod: Mod[T])
      (reader: T => (Changeable[U], Changeable[V]))
      (implicit c: Context): (Changeable[U], Changeable[V]) = {
    val value = c.read(mod, c.task.self)

    val time = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c.currentModId,
      c.currentModId2,
      c)

    val changeables = reader(value)

    val end = c.ddg.nextTimestamp(Timestamp.getNodePtr(time), c)
    Timestamp.setEndPtr(time, end)

    changeables
  }

  def read_2[T, U, V](mod1: Mod[T], mod2: Mod[U])
      (reader: (T, U) => Changeable[V])
      (implicit c: Context): Changeable[V] = {
    val value1 = c.read(mod1, c.task.self)
    val value2 = c.read(mod2, c.task.self)

    val time = c.ddg.addRead2(
      mod1.asInstanceOf[Mod[Any]],
      mod2.asInstanceOf[Mod[Any]],
      value1,
      value2,
      reader.asInstanceOf[(Any, Any) => Changeable[Any]],
      c.currentModId,
      -1,
      c)

    val changeable = reader(value1, value2)

    val end = c.ddg.nextTimestamp(Timestamp.getNodePtr(time), c)
    Timestamp.setEndPtr(time, end)

    changeable
  }

  def mod[T](initializer: => Changeable[T])
     (implicit c: Context): Mod[T] = {
    val mod1 = new Mod[T](c.newModId())

    modHelper(initializer, mod1.id, -1, -1, -1, c)

    mod1
  }

  def modHelper[T, U]
      (initializer: => Any,
       modId1: ModId,
       modId2: ModId,
       currentModId: ModId,
       currentModId2: ModId,
       c: Context): Unit = {
    val oldCurrentModId = c.currentModId
    if (modId1 != -1) {
      c.currentModId = modId1
    }

    val oldCurrentModId2 = c.currentModId2
    if (modId2 != -1) {
      c.currentModId2 = modId2
    }

    val modNodePointer = ModNode.create(
      modId1, modId2, currentModId, currentModId2)

    val time = c.ddg.nextTimestamp(modNodePointer, c)

    initializer

    val end = c.ddg.nextTimestamp(modNodePointer, c)
    Timestamp.setEndPtr(time, end)

    c.currentModId = oldCurrentModId
    c.currentModId2 = oldCurrentModId2
  }

  def modizerHelper[T, U]
      (initializer: => Any,
       modId1: ModId,
       modId2: ModId,
       modizerId: ModizerId,
       key: Any,
       currentModId: ModId,
       currentModId2: ModId,
       c: Context): Unit = {
    val oldCurrentModId = c.currentModId
    if (modId1 != -1) {
      c.currentModId = modId1
    }

    val oldCurrentModId2 = c.currentModId2
    if (modId2 != -1) {
      c.currentModId2 = modId2
    }

    val modNodePointer = ModizerNode.create(
      modId1, modId2, modizerId, key, currentModId, currentModId2)

    val time = c.ddg.nextTimestamp(modNodePointer, c)

    initializer

    val end = c.ddg.nextTimestamp(modNodePointer, c)
    Timestamp.setEndPtr(time, end)

    c.currentModId = oldCurrentModId
    c.currentModId2 = oldCurrentModId2
  }

  def mod2[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) = {
    val mod1 = new Mod[T](c.newModId())
    val mod2 = new Mod[U](c.newModId())

    modHelper(initializer, mod1.id, mod2.id, -1, -1, c)

    (mod1, mod2)
  }

  def modLeft[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) = {
    val modLeft = new Mod[T](c.newModId())

    modHelper(initializer, modLeft.id, -1, -1, c.currentModId2, c)

    (modLeft, new Changeable[U](c.currentModId2))
  }

  def modRight[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) = {
    val modRight = new Mod[U](c.newModId())
    modHelper(initializer, -1, modRight.id, c.currentModId, -1, c)

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

  def parWithHint[T, U](one: Context => T, hint1: WorkerId = -1)
      (two: Context => U, hint2: WorkerId = -1)
      (implicit c: Context): (T, U) = {
    val future1 = c.masterRef ? ScheduleTaskMessage(c.task.self, hint1)
    val (taskId1, taskRef1) =
      Await.result(future1.mapTo[(TaskId, ActorRef)], DURATION)

    val adjust1 = new Adjustable[T] { def run(implicit c: Context) = one(c) }
    val oneFuture = taskRef1 ? RunTaskMessage(adjust1)

    val future2 = c.masterRef ? ScheduleTaskMessage(c.task.self, hint2)
    val (taskId2, taskRef2) =
      Await.result(future2.mapTo[(TaskId, ActorRef)], DURATION)

    val adjust2 = new Adjustable[U] { def run(implicit c: Context) = two(c) }
    val twoFuture = taskRef2 ? RunTaskMessage(adjust2)

    val parNode = c.ddg.addPar(taskRef1, taskRef2, taskId1, taskId2, c)

    val oneRet = Await.result(oneFuture, DURATION).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, DURATION).asInstanceOf[U]
    (oneRet, twoRet)
  }

  def par[T](one: Context => T): Parizer[T] = {
    new Parizer(one)
  }
}
