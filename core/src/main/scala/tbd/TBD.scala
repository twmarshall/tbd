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
import scala.concurrent.{Await, Future}

import tbd.macros.TbdMacros

import tbd.Constants._
import tbd.master.Main
import tbd.messages._
import tbd.mod.{Dest, Mod}
import tbd.ddg.FunctionTag
import tbd.TBD._

object TBD {

  import scala.language.experimental.macros
  def read[T, U](
      mod: Mod[T])
     (reader: T => Changeable[U])
     (implicit c: Context):
    Changeable[U] =
      macro TbdMacros.readMacro[Changeable[U]]

  def readInternal[T, U](
      mod: Mod[T],
      reader: T => Changeable[U],
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): Changeable[U] = {

    val value = mod.read(c.worker.self)

    val readNode = c.worker.ddg.addRead(
        mod.asInstanceOf[Mod[Any]], value, c.currentParent,
        reader.asInstanceOf[Any => Changeable[Any]],
        FunctionTag(readerId, freeTerms))

    val outerReader = c.currentParent
    c.currentParent = readNode

    val changeable = reader(value)
    c.currentParent = outerReader

    readNode.endTime = c.worker.ddg.nextTimestamp(readNode)
    readNode.currentDest = c.currentDest
    readNode.currentDest2 = c.currentDest2

    changeable
  }

  def read_2[T, U, V](
      mod: Mod[T])
     (reader: T => (Changeable[U], Changeable[V]))
     (implicit c: Context):
    (Changeable[U], Changeable[V]) =
      macro TbdMacros.read_2Macro[(Changeable[U], Changeable[V])]

  def read_2Internal[T, U, V](
      mod: Mod[T],
      reader: T => (Changeable[U], Changeable[V]),
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): (Changeable[U], Changeable[V]) = {

    val value = mod.read(c.worker.self)

    val readNode = c.worker.ddg.addRead(mod.asInstanceOf[Mod[Any]], value,
                                        c.currentParent,
                                        reader.asInstanceOf[Any => Changeable[Any]],
                                        FunctionTag(readerId, freeTerms))

    val outerReader = c.currentParent
    c.currentParent = readNode

    val changeables = reader(value)
    c.currentParent = outerReader

    readNode.endTime = c.worker.ddg.nextTimestamp(readNode)
    readNode.currentDest = c.currentDest
    readNode.currentDest2 = c.currentDest2

    changeables
  }

  def read2[T, V, U](
      a: Mod[T],
      b: Mod[V])
     (reader: (T, V) => (Changeable[U]))
     (implicit c: Context): Changeable[U] = {
    readInternal(a, (a: T) => {
        readInternal(b, (b: V) => { reader(a, b) }, c, -1, List[(String, Any)]())
    }, c, -1, List[(String, Any)]())
  }

  def mod[T](
      initializer: => Changeable[T])
     (implicit c: Context): Mod[T] = macro TbdMacros.modMacro[Mod[T]]

  def modInternal[T](
      initializer: => Changeable[T],
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): Mod[T] = {

    val oldCurrentDest = c.currentDest
    c.currentDest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]

    if(Main.debug) {
      val modNode = c.worker.ddg.addMod(c.currentDest.mod, c.currentParent,
                                        FunctionTag(readerId, freeTerms))

      val outerReader = c.currentParent
      c.currentParent = modNode

      initializer

      c.currentParent = outerReader
      modNode.endTime = c.worker.ddg.nextTimestamp(modNode)
    } else {
      initializer
    }

    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest

    mod.asInstanceOf[Mod[T]]
  }

  def mod2[T, U](
      initializer: => (Changeable[T], Changeable[U]),
      key: Any = null)
     (implicit c: Context): (Mod[T], Mod[U]) = {
    val oldCurrentDest = c.currentDest

    c.currentDest =
      if (key != null) {
	if (c.allocations.contains(key)) {
	  c.allocations(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  c.allocations(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }


    val oldCurrentDest2 = c.currentDest2

    c.currentDest2 =
      if (key != null) {
	if (c.allocations2.contains(key)) {
	  c.allocations2(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  c.allocations2(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    initializer

    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest
    val mod2 = c.currentDest2.mod
    c.currentDest2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
  }

  def modLeft[T, U](
      initializer: => (Changeable[T], Changeable[U]),
      key: Any = null)
     (implicit c: Context): (Mod[T], Changeable[U]) = {
    val oldCurrentDest = c.currentDest
    c.currentDest =
      if (key != null) {
	if (c.allocations.contains(key)) {
	  c.allocations(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  c.allocations(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    initializer

    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest

    (mod.asInstanceOf[Mod[T]],
     new Changeable(c.currentDest2.mod.asInstanceOf[Mod[U]]))
  }

  def modRight[T, U](
      initializer: => (Changeable[T], Changeable[U]),
      key: Any = null)
     (implicit c: Context): (Changeable[T], Mod[U]) = {
    val oldCurrentDest2 = c.currentDest2
    c.currentDest2 =
      if (key != null) {
	if (c.allocations2.contains(key)) {
	  c.allocations2(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  c.allocations2(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    initializer

    val mod2 = c.currentDest2.mod
    c.currentDest2 = oldCurrentDest2

    (new Changeable(c.currentDest.mod.asInstanceOf[Mod[T]]),
     mod2.asInstanceOf[Mod[U]])
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    import c.worker.context.dispatcher

    val awaiting = c.currentDest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    val changeable = new Changeable(c.currentDest.mod)
    if (Main.debug) {
      val writeNode = c.worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                            c.currentParent)
      writeNode.endTime = c.worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable[T]]
  }

  def write2[T, U](
      value: T,
      value2: U)
     (implicit c: Context): (Changeable[T], Changeable[U]) = {
    import c.worker.context.dispatcher

    val awaiting = c.currentDest.mod.update(value)
    val awaiting2 = c.currentDest2.mod.update(value2)
    Await.result(Future.sequence(awaiting), DURATION)
    Await.result(Future.sequence(awaiting2), DURATION)

    write2Helper(c)
  }

  def writeLeft[T, U](
      value: T,
      changeable: Changeable[U])
     (implicit c: Context): (Changeable[T], Changeable[U]) = {
    import c.worker.context.dispatcher

    if (changeable.mod != c.currentDest2.mod) {
      println("WARNING - mod parameter to writeLeft doesn't match currentDest2")
    }

    val awaiting = c.currentDest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    write2Helper(c)
  }

  def writeRight[T, U](
      changeable: Changeable[T],
      value2: U)
     (implicit c: Context): (Changeable[T], Changeable[U]) = {
    import c.worker.context.dispatcher

    if (changeable.mod != c.currentDest.mod) {
      println("WARNING - mod parameter to writeRight doesn't match currentDest")
    }

    val awaiting = c.currentDest2.mod.update(value2)
    Await.result(Future.sequence(awaiting), DURATION)

    write2Helper(c)
  }

  private def write2Helper[T, U](
      c: Context): (Changeable[T], Changeable[U]) = {
    if (Main.debug) {
      val writeNode = c.worker.ddg.addWrite(c.currentDest.mod.asInstanceOf[Mod[Any]],
                                            c.currentParent)
      writeNode.mod2 = c.currentDest2.mod
      writeNode.endTime = c.worker.ddg.nextTimestamp(writeNode)
    }

    (new Changeable(c.currentDest.mod).asInstanceOf[Changeable[T]],
     new Changeable(c.currentDest2.mod).asInstanceOf[Changeable[U]])
  }

  def par[T](one: Context => T): Parer[T] = macro TbdMacros.parOneMacro[Parer[T]]

  def parInternal[T](
      one: Context => T,
      id: Int,
      closedTerms: List[(String, Any)]): Parer[T] = {
    new Parer(one, id, closedTerms)
  }

  def makeMemoizer[T](dummy: Boolean = false)(implicit c: Context): Memoizer[T] = {
    c.nextMemoId += 1

    if(dummy) {
      new DummyMemoizer[T](c, c.nextMemoId)
    } else {
      new Memoizer[T](c, c.nextMemoId)
    }
  }

  def createMod[T](value: T)(implicit c: Context): Mod[T] = {
    mod {
      write(value)
    }
  }
}
