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
import tbd.datastore.DependencyManager
import tbd.ddg.{FunctionTag, Tag}
import tbd.TBD._

object TBD {
  import scala.language.experimental.macros

  def read[T, U](mod: Mod[T])
      (reader: T => Changeable[U])
      (implicit c: Context): Changeable[U] = {
    val value = mod.read(c.worker.self)

    val readNode = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      c.currentParent,
      reader.asInstanceOf[Any => Changeable[Any]])

    val outerReader = c.currentParent
    c.currentParent = readNode

    val changeable = reader(value)

    c.currentParent = outerReader
    readNode.endTime = c.ddg.nextTimestamp(readNode)
    readNode.currentMod = c.currentMod

    changeable
  }

  def read2[T, U, V](mod: Mod[T])
      (reader: T => (Changeable[U], Changeable[V]))
      (implicit c: Context): (Changeable[U], Changeable[V]) = {
    val value = mod.read(c.worker.self)

    val readNode = c.ddg.addRead(
      mod.asInstanceOf[Mod[Any]],
      value,
      c.currentParent,
      reader.asInstanceOf[Any => Changeable[Any]])

    val outerReader = c.currentParent
    c.currentParent = readNode

    val changeables = reader(value)

    c.currentParent = outerReader
    readNode.endTime = c.ddg.nextTimestamp(readNode)
    readNode.currentMod = c.currentMod
    readNode.currentMod2 = c.currentMod2

    changeables
  }

  def makeModizer[T]() = new Modizer1[T]()

  def makeModizer2[T, U]() = new Modizer2[T, U]()

  @functionToInvoke("modInternal")
  def mod[T](initializer: => Changeable[T])
     (implicit c: Context): Mod[T] = {
    modInternal(initializer, c, 0, null)
  }

  def modInternal[T](
      initializer: => Changeable[T],
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): Mod[T] = {
    val mod1 = new Mod[T](c.newModId())

    modWithDest(initializer, mod1, null, null, c, readerId, freeTerms)
  }

  def modWithDest[T](
      initializer: => Changeable[T],
      mod1: Mod[T],
      modizer: Modizer1[T],
      key: Any,
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): Mod[T] = {
    val oldCurrentDest = c.currentMod

    c.currentMod = mod1.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      c.currentMod,
      null,
      c.currentParent,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      FunctionTag(readerId, freeTerms))

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.ddg.nextTimestamp(modNode)

    val mod = c.currentMod
    c.currentMod = oldCurrentDest

    mod.asInstanceOf[Mod[T]]
  }

  @functionToInvoke("mod2Internal")
  def mod2[T, U]
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) =
    macro TbdMacros.modMacro[(Mod[T], Mod[U])]

  def mod2Internal[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Mod[U]) = {
    mod2WithDests(
      initializer,
      new Mod[T](c.newModId()),
      new Mod[U](c.newModId()),
      null,
      null,
      c,
      readerId,
      freeTerms)
  }

  def mod2WithDests[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       modLeft: Mod[T],
       modRight: Mod[U],
       modizer: Modizer2[T, U],
       key: Any,
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Mod[U]) = {
    val oldCurrentDest = c.currentMod
    c.currentMod = modLeft.asInstanceOf[Mod[Any]]

    val oldCurrentDest2 = c.currentMod2
    c.currentMod2 = modRight.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      c.currentMod,
      c.currentMod2,
      c.currentParent,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      FunctionTag(readerId, freeTerms))

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.ddg.nextTimestamp(modNode)

    val mod = c.currentMod
    c.currentMod = oldCurrentDest
    val mod2 = c.currentMod2
    c.currentMod2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
  }

  @functionToInvoke("modLeftInternal")
  def modLeft[T, U]
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) =
    macro TbdMacros.modMacro[(Mod[T], Changeable[U])]

  def modLeftInternal[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Changeable[U]) = {
    modLeftWithDest(initializer, new Mod[T](c.newModId()), null, null, c, readerId, freeTerms)
  }

  def modLeftWithDest[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       modLeft: Mod[T],
       modizer: Modizer2[T, U],
       key: Any,
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Changeable[U]) = {

    val oldCurrentDest = c.currentMod
    c.currentMod = modLeft.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      c.currentMod,
      null,
      c.currentParent,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      FunctionTag(readerId, freeTerms))

    modNode.currentMod2 = c.currentMod2

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.ddg.nextTimestamp(modNode)

    val mod = c.currentMod
    c.currentMod = oldCurrentDest

    (mod.asInstanceOf[Mod[T]],
     new Changeable(c.currentMod2.asInstanceOf[Mod[U]]))
  }

  @functionToInvoke("modRightInternal")
  def modRight[T, U]
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) =
    macro TbdMacros.modMacro[(Changeable[T], Mod[U])]

  def modRightInternal[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Changeable[T], Mod[U]) = {
    modRightWithDest(initializer, new Mod[U](c.newModId()), null, null, c, readerId, freeTerms)
  }

  def modRightWithDest[T, U]
      (initializer: => (Changeable[T], Changeable[U]),
       modRight: Mod[U],
       modizer: Modizer2[T, U],
       key: Any,
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Changeable[T], Mod[U]) = {
    val oldCurrentDest2 = c.currentMod2
    c.currentMod2 = modRight.asInstanceOf[Mod[Any]]

    val modNode = c.ddg.addMod(
      null,
      c.currentMod2,
      c.currentParent,
      modizer.asInstanceOf[Modizer[Any]],
      key,
      FunctionTag(readerId, freeTerms))

    modNode.currentMod = c.currentMod

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.ddg.nextTimestamp(modNode)

    val mod2 = c.currentMod2
    c.currentMod2 = oldCurrentDest2

    (new Changeable(c.currentMod.asInstanceOf[Mod[T]]),
     mod2.asInstanceOf[Mod[U]])
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    if (c.currentMod.update(value) && !c.initialRun) {
      c.pending += DependencyManager.modUpdated(c.currentMod.id, c.worker.self)

      if (c.ddg.reads.contains(c.currentMod.id)) {
	c.updatedMods += c.currentMod.id
	c.ddg.modUpdated(c.currentMod.id)
      }
    }

    (new Changeable(c.currentMod)).asInstanceOf[Changeable[T]]
  }

  def write2[T, U](value: T, value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    if (c.currentMod.update(value) && !c.initialRun) {
      c.pending += DependencyManager.modUpdated(c.currentMod.id, c.worker.self)

      if (c.ddg.reads.contains(c.currentMod.id)) {
	c.updatedMods += c.currentMod.id
	c.ddg.modUpdated(c.currentMod.id)
      }
    }

    if (c.currentMod2.update(value2) && !c.initialRun) {
      c.pending += DependencyManager.modUpdated(c.currentMod2.id, c.worker.self)

      if (c.ddg.reads.contains(c.currentMod2.id)) {
	c.updatedMods += c.currentMod2.id
	c.ddg.modUpdated(c.currentMod2.id)
      }
    }

    (new Changeable(c.currentMod).asInstanceOf[Changeable[T]],
     new Changeable(c.currentMod2).asInstanceOf[Changeable[U]])
  }

  def writeLeft[T, U](value: T, changeable: Changeable[U])
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    if (c.currentMod.update(value) && !c.initialRun) {
      c.pending += DependencyManager.modUpdated(c.currentMod.id, c.worker.self)

      if (c.ddg.reads.contains(c.currentMod.id)) {
	c.updatedMods += c.currentMod.id
	c.ddg.modUpdated(c.currentMod.id)
      }
    }

    (new Changeable(c.currentMod).asInstanceOf[Changeable[T]],
     new Changeable(c.currentMod2).asInstanceOf[Changeable[U]])
  }

  def writeRight[T, U](changeable: Changeable[T], value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    if (c.currentMod2.update(value2) && !c.initialRun) {
      c.pending += DependencyManager.modUpdated(c.currentMod2.id, c.worker.self)

      if (c.ddg.reads.contains(c.currentMod2.id)) {
	c.updatedMods += c.currentMod2.id
	c.ddg.modUpdated(c.currentMod2.id)
      }
    }

    (new Changeable(c.currentMod).asInstanceOf[Changeable[T]],
     new Changeable(c.currentMod2).asInstanceOf[Changeable[U]])
  }

  @functionToInvoke("parInternal")
  def par[T](one: Context => T): Parer[T] = macro TbdMacros.parOneMacro[Parer[T]]

  def parInternal[T](
      one: Context => T,
      id: Int,
      closedTerms: List[(String, Any)]): Parer[T] = {
    new Parer(one, id, closedTerms)
  }

  def makeMemoizer[T](dummy: Boolean = false)(implicit c: Context): Memoizer[T] = {
    if(dummy) {
      new DummyMemoizer[T](c)
    } else {
      new Memoizer[T](c)
    }
  }

  def createMod[T](value: T)(implicit c: Context): Mod[T] = {
    mod {
      write(value)
    }
  }
}
