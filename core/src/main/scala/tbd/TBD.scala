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

import akka.pattern.ask
import scala.collection.mutable.ListBuffer

import tbd.macros.{TbdMacros, functionToInvoke}

import tbd.Constants._
import tbd.master.Main
import tbd.messages._
import tbd.ddg.{FunctionTag, Tag}
import tbd.TBD._

object TBD {
  import scala.language.experimental.macros

  def read[T, U](mod: Mod[T])
      (reader: T => Changeable[U])
      (implicit c: Context): Changeable[U] = {
    val value = c.read(mod, c.worker.self)

    val readNode = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c)

    val changeable = reader(value)

    readNode.endTime = c.ddg.nextTimestamp(readNode, c)
    readNode.currentMod = c.currentMod

    changeable
  }

  def read2[T, U, V](mod: Mod[T])
      (reader: T => (Changeable[U], Changeable[V]))
      (implicit c: Context): (Changeable[U], Changeable[V]) = {
    val value = c.read(mod, c.worker.self)

    val readNode = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      reader.asInstanceOf[Any => Changeable[Any]],
      c)

    val changeables = reader(value)

    readNode.endTime = c.ddg.nextTimestamp(readNode, c)
    readNode.currentMod = c.currentMod
    readNode.currentMod2 = c.currentMod2

    changeables
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
    val outerCurrentMod = c.currentMod

    c.currentMod = mod1.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)

    initializer

    modNode.endTime = c.ddg.nextTimestamp(modNode, c)

    val mod = c.currentMod
    c.currentMod = outerCurrentMod

    mod.asInstanceOf[Mod[T]]
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
    val oldCurrentDest = c.currentMod
    c.currentMod = modLeft.asInstanceOf[Mod[Any]]

    val oldCurrentDest2 = c.currentMod2
    c.currentMod2 = modRight.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)

    initializer

    modNode.endTime = c.ddg.nextTimestamp(modNode, c)

    val mod = c.currentMod
    c.currentMod = oldCurrentDest
    val mod2 = c.currentMod2
    c.currentMod2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
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

    val oldCurrentDest = c.currentMod
    c.currentMod = modLeft.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)

    modNode.currentMod2 = c.currentMod2

    initializer

    modNode.endTime = c.ddg.nextTimestamp(modNode, c)

    val mod = c.currentMod
    c.currentMod = oldCurrentDest

    (mod.asInstanceOf[Mod[T]],
     new Changeable(c.currentMod2.asInstanceOf[Mod[U]]))
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
    val oldCurrentDest2 = c.currentMod2
    c.currentMod2 = modRight.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      modizer.asInstanceOf[Modizer[Any]],
      key,
      c)

    modNode.currentMod = c.currentMod

    initializer

    modNode.endTime = c.ddg.nextTimestamp(modNode, c)

    val mod2 = c.currentMod2
    c.currentMod2 = oldCurrentDest2

    (new Changeable(c.currentMod.asInstanceOf[Mod[T]]),
     mod2.asInstanceOf[Mod[U]])
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    c.update(c.currentMod, value)

    (new Changeable(c.currentMod)).asInstanceOf[Changeable[T]]
  }

  def write2[T, U](value: T, value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    c.update(c.currentMod, value)

    c.update(c.currentMod2, value2)

    (new Changeable(c.currentMod).asInstanceOf[Changeable[T]],
     new Changeable(c.currentMod2).asInstanceOf[Changeable[U]])
  }

  def writeLeft[T, U](value: T, changeable: Changeable[U])
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    c.update(c.currentMod, value)

    (new Changeable(c.currentMod).asInstanceOf[Changeable[T]],
     new Changeable(c.currentMod2).asInstanceOf[Changeable[U]])
  }

  def writeRight[T, U](changeable: Changeable[T], value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    c.update(c.currentMod2, value2)

    (new Changeable(c.currentMod).asInstanceOf[Changeable[T]],
     new Changeable(c.currentMod2).asInstanceOf[Changeable[U]])
  }

  def par[T](one: Context => T): Parizer[T] = {
    new Parizer(one)
  }
}
