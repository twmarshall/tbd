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
import scala.collection.mutable.ListBuffer
import scala.concurrent.Await

import tdb.Constants._
import tdb.list.ListInput
import tdb.messages._
import tdb.TDB._

object DummyTDB {
  def put[T, U](input: ListInput[T, U], key: T, value: U)
      (implicit c: Context) {
    val future = input.asyncPut(key, value)
    if (!c.initialRun) {
      c.pending += future
    }
  }

  def read[T, U](mod: Mod[T])
      (reader: T => Changeable[U])
      (implicit c: Context): Changeable[U] = {
    val value = c.read(mod, c.task.self)

    val changeable = reader(value)

    changeable
  }

  def readAny[T](mod: Mod[T])
      (reader: T => Any)
      (implicit c: Context): Any = {
    val value = c.read(mod, c.task.self)

    val ret = reader(value)

    ret
  }

  def read2[T, U, V](mod: Mod[T])
      (reader: T => (Changeable[U], Changeable[V]))
      (implicit c: Context): (Changeable[U], Changeable[V]) = {
    val value = c.read(mod, c.task.self)

    val changeables = reader(value)

    changeables
  }

  def read_2[T, U, V](mod1: Mod[T], mod2: Mod[U])
      (reader: (T, U) => Changeable[V])
      (implicit c: Context): Changeable[V] = {
    val value1 = c.read(mod1, c.task.self)
    val value2 = c.read(mod2, c.task.self)

    val changeable = reader(value1, value2)

    changeable
  }

  def mod[T](initializer: => Changeable[T])
     (implicit c: Context): Mod[T] = {
    val mod1 = new Mod[T](c.newModId())

    initializer

    mod1
  }

  def mod2[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) = {
    val mod1 = new Mod[T](c.newModId())
    val mod2 = new Mod[U](c.newModId())

    initializer

    (mod1, mod2)
  }

  def modLeft[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) = {
    val mod = new Mod[T](c.newModId())

    initializer

    (mod, new Changeable[U](c.currentModId2))
  }

  def modRight[T, U](initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) = {
    val mod = new Mod[U](c.newModId())

    initializer

    (new Changeable[T](c.currentModId), mod)
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

    val oneRet = Await.result(oneFuture, DURATION).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, DURATION).asInstanceOf[U]

    (oneRet, twoRet)
  }

  def par[T](one: Context => T): Parizer[T] = {
    new Parizer(one)
  }
}
