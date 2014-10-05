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
package tbd.debug

import akka.pattern.ask
import scala.collection.mutable.{ListBuffer, Map}

import tbd.Constants._
import tbd.macros.{TbdMacros, functionToInvoke}
import tbd.ddg._

object TBD {
  import scala.language.experimental.macros

  type Adjustable[T] = tbd.Adjustable[T]
  type Changeable[T] = tbd.Changeable[T]
  type Context = tbd.Context
  type Mod[T] = tbd.Mod[T]

  val tags = Map[Node, Tag]()

  @functionToInvoke("readInternal")
  def read[T, U]
      (mod: Mod[T])
      (reader: T => Changeable[U])
      (implicit c: Context): Changeable[U] =
    macro TbdMacros.readMacro[Changeable[U]]

  def readInternal[T, U]
      (mod: Mod[T],
       reader: T => Changeable[U],
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): Changeable[U] = {
    val tag = Tag.Read(mod.read(), FunctionTag(readerId, freeTerms))(mod.id)

    val changeable = tbd.TBD.read(mod)(reader)(c)

    val readNode = c.ddg.reads(mod.id).last
    tags(readNode) = tag

    changeable
  }

  @functionToInvoke("read2Internal")
  def read2[T, U, V]
      (mod: Mod[T])
      (reader: T => (Changeable[U], Changeable[V]))
      (implicit c: Context): (Changeable[U], Changeable[V]) =
    macro TbdMacros.readMacro[(Changeable[U], Changeable[V])]

  def read2Internal[T, U, V](
      mod: Mod[T],
      reader: T => (Changeable[U], Changeable[V]),
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]): (Changeable[U], Changeable[V]) = {
    val tag = Tag.Read(mod.read(), FunctionTag(readerId, freeTerms))(mod.id)

    val changeable = tbd.TBD.read2(mod)(reader)(c)

    val readNode = c.ddg.reads(mod.id).last
    tags(readNode) = tag

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
    val mod = tbd.TBD.mod(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    tags(modNode) = tag

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
    val (mod1, mod2) = tbd.TBD.mod2(initializer)(c)

    val tag = Tag.Mod(List(mod1.id, mod2.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    tags(modNode) = tag

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
    val (mod, changeable) = tbd.TBD.modLeft(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    tags(modNode) = tag

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
    val (changeable, mod) = tbd.TBD.modRight(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    tags(modNode) = tag

    (changeable, mod)
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    val changeable = tbd.TBD.write(value)
    val mod = changeable.mod.asInstanceOf[Mod[Any]]

    val writeNode = c.ddg.addWrite(mod, null, c)
    writeNode.endTime = c.ddg.nextTimestamp(writeNode, c)

    val writes = List(SingleWriteTag(mod.id, mod.read()))
    val tag = Tag.Write(writes)
    tags(writeNode) = tag

    changeable
  }

  def write2[T, U](value: T, value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    val changeables = tbd.TBD.write2(value, value2)
    val mod1 = c.currentMod.asInstanceOf[Mod[Any]]
    val mod2 = c.currentMod2.asInstanceOf[Mod[Any]]

    val writeNode = c.ddg.addWrite(mod1, mod2, c)

    writeNode.endTime = c.ddg.nextTimestamp(writeNode, c)

    val writes = List(
      SingleWriteTag(mod1.id, mod1.read()),
      SingleWriteTag(mod1.id, mod1.read()))
    val tag = Tag.Write(writes)
    tags(writeNode) = tag

    changeables
  }

  def writeLeft[T, U](value: T, changeable: Changeable[U])
     (implicit c: Context): (Changeable[T], Changeable[U]) = {
    if (changeable.mod != c.currentMod2) {
      println("WARNING - mod parameter to writeLeft doesn't match currentMod2")
    }

    val changeables = tbd.TBD.writeLeft(value, changeable)
    val mod = c.currentMod.asInstanceOf[Mod[Any]]

    val writeNode = c.ddg.addWrite(mod, null, c)

    writeNode.endTime = c.ddg.nextTimestamp(writeNode, c)

    val writes = List(SingleWriteTag(mod.id, mod.read()))
    val tag = Tag.Write(writes)
    tags(writeNode) = tag

    changeables
  }

  def writeRight[T, U](changeable: Changeable[T], value2: U)
      (implicit c: Context): (Changeable[T], Changeable[U]) = {
    if (changeable.mod != c.currentMod) {
      println("WARNING - mod parameter to writeRight doesn't match currentMod")
    }

    val changeables = tbd.TBD.writeRight(changeable, value2)
    val mod2 = c.currentMod2.asInstanceOf[Mod[Any]]

    val writeNode = c.ddg.addWrite(null, mod2, c)

    writeNode.endTime = c.ddg.nextTimestamp(writeNode, c)

    val writes = List(SingleWriteTag(mod2.id, mod2.read()))
    val tag = Tag.Write(writes)
    tags(writeNode) = tag

    changeables
  }

  @functionToInvoke("parInternal")
  def par[T](one: Context => T): Parizer[T] =
	macro TbdMacros.parOneMacro[Parizer[T]]

  def parInternal[T](
      one: Context => T,
      id: Int,
      closedTerms: List[(String, Any)]): Parizer[T] = {
    new Parizer(one, id, closedTerms)
  }
}
