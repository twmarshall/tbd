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
import tbd.ddg._
import tbd.TBD._

object DebugTBD {
  import scala.language.experimental.macros

  @functionToInvoke("readInternal")
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
    val tag = Tag.Read(mod.read(), FunctionTag(readerId, freeTerms))(mod.id)

    val changeable = TBD.read(mod)(reader)(c)

    val readNode = c.ddg.reads(mod.id).last
    readNode.tag = tag

    changeable
  }

  @functionToInvoke("read2Internal")
  def read2[T, U, V](
      mod: Mod[T])
     (reader: T => (Changeable[U], Changeable[V]))
     (implicit c: Context):
    (Changeable[U], Changeable[V]) =
      macro TbdMacros.readMacro[(Changeable[U], Changeable[V])]

  def read2Internal[T, U, V](
      mod: Mod[T],
      reader: T => (Changeable[U], Changeable[V]),
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): (Changeable[U], Changeable[V]) = {
    val tag = Tag.Read(mod.read(), FunctionTag(readerId, freeTerms))(mod.id)

    val changeable = TBD.read2(mod)(reader)(c)

    val readNode = c.ddg.reads(mod.id).last
    readNode.tag = tag

    changeable
  }

  @functionToInvoke("modInternal")
  def mod[T](initializer: => Changeable[T])
     (implicit c: Context): Mod[T] = macro TbdMacros.modMacro[Mod[T]]

  def modInternal[T](
      initializer: => Changeable[T],
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): Mod[T] = {
    val mod = TBD.mod(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentParent.children.last.asInstanceOf[ModNode]
    modNode.tag = tag

    mod
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
    val (mod1, mod2) = TBD.mod2(initializer)(c)

    val tag = Tag.Mod(List(mod1.id, mod2.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentParent.children.last.asInstanceOf[ModNode]
    modNode.tag = tag

    (mod1, mod2)
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
    val (mod, changeable) = TBD.modLeft(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentParent.children.last.asInstanceOf[ModNode]
    modNode.tag = tag

    (mod, changeable)
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
    val (changeable, mod) = TBD.modRight(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentParent.children.last.asInstanceOf[ModNode]
    modNode.tag = tag

    (changeable, mod)
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    val changeable = TBD.write(value)
    val writeNode = c.ddg.addWrite(
      changeable.mod.asInstanceOf[Mod[Any]],
      null,
      c)
    writeNode.endTime = c.ddg.nextTimestamp(writeNode, writeNode, c)

    changeable
  }

  def write2[T, U](value: T, value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    val changeables = TBD.write2(value, value2)

    val writeNode = c.ddg.addWrite(
      c.currentMod.asInstanceOf[Mod[Any]],
      c.currentMod2.asInstanceOf[Mod[Any]],
      c)

    writeNode.endTime = c.ddg.nextTimestamp(writeNode, writeNode, c)

    changeables
  }

  def writeLeft[T, U](value: T, changeable: Changeable[U])
     (implicit c: Context): (Changeable[T], Changeable[U]) = {
    if (changeable.mod != c.currentMod2) {
      println("WARNING - mod parameter to writeLeft doesn't match currentMod2")
    }

    val changeables = TBD.writeLeft(value, changeable)

    val writeNode = c.ddg.addWrite(
      c.currentMod.asInstanceOf[Mod[Any]],
      null,
      c)

    writeNode.endTime = c.ddg.nextTimestamp(writeNode, writeNode, c)

    changeables
  }

  def writeRight[T, U](changeable: Changeable[T], value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    if (changeable.mod != c.currentMod) {
      println("WARNING - mod parameter to writeRight doesn't match currentMod")
    }

    val changeables = TBD.writeRight(changeable, value2)

    val writeNode = c.ddg.addWrite(
      null,
      c.currentMod2.asInstanceOf[Mod[Any]],
      c)

    writeNode.endTime = c.ddg.nextTimestamp(writeNode, writeNode, c)

    changeables
  }

  @functionToInvoke("parInternal")
  def par[T](one: Context => T): Parizer[T] =
	macro TbdMacros.parOneMacro[Parizer[T]]

  def parInternal[T](
      one: Context => T,
      id: Int,
      closedTerms: List[(String, Any)]): DebugParizer[T] = {
    new DebugParizer(one, id, closedTerms)
  }
}
