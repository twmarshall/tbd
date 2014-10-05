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
import scala.collection.mutable.Map
import scala.concurrent.Await

import tbd._
import tbd.Constants._
import tbd.ddg.{FunctionTag, ModNode, Node, Tag}
import tbd.macros.{TbdMacros, functionToInvoke}
import tbd.messages._

class Modizer1[T] extends Modizer[T] {
  import scala.language.experimental.macros

  val allocations = Map[Any, Mod[T]]()

  def remove(key: Any) {
    allocations -= key
  }

  @functionToInvoke("applyInternal")
  def apply(key: Any)
      (initializer: => Changeable[T])
      (implicit c: Context): Mod[T] = macro TbdMacros.modizerMacro[Mod[T]]

  def applyInternal
      (key: Any,
       initializer: => Changeable[T],
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]) = {
    val mod1 =
      if (allocations.contains(key)) {
	allocations(key)
      } else {
	val mod1 = new Mod[T](c.newModId())
	allocations(key) = mod1
	mod1
      }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val mod = tbd.TBD.modInternal(initializer, mod1, this, key, c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    TBD.nodes(modNode) = (internalId, tag, stack)

    mod
  }
}

class Modizer2[T, U] extends tbd.Modizer2[T, U] {
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  override def apply
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) =
    macro TbdMacros.modizerMacro[(Mod[T], Mod[U])]

  def applyInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]) = {
    val modLeft =
      if (allocations.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations(key)
      } else {
	val modLeft = new Mod[T](c.newModId())
	allocations(key) = modLeft
	modLeft
      }

    val modRight =
      if (allocations2.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations2(key)
      } else {
	val modRight = new Mod[U](c.newModId())
	allocations2(key) = modRight
	modRight
      }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (mod1, mod2) =
      tbd.TBD.mod2Internal(initializer, modLeft, modRight, this, key, c)

    val tag = Tag.Mod(List(mod1.id, mod2.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    TBD.nodes(modNode) = (internalId, tag, stack)

    (mod1, mod2)
  }

  @functionToInvoke("modLeftInternal")
  override def left(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) =
    macro TbdMacros.modizerMacro[(Mod[T], Changeable[U])]

  def modLeftInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Changeable[U]) = {
    val modLeft =
      if (allocations.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations(key)
      } else {
	val modLeft = new Mod[T](c.newModId())
	allocations(key) = modLeft
	modLeft
      }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (mod, changeable) =
      tbd.TBD.modLeftInternal(initializer, modLeft, this, key, c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    TBD.nodes(modNode) = (internalId, tag, stack)

    (mod, changeable)
  }

  @functionToInvoke("modRightInternal")
  override def right(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) =
    macro TbdMacros.modizerMacro[(Changeable[T], Mod[U])]

  def modRightInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Changeable[T], Mod[U]) = {
    val modRight =
      if (allocations2.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations2(key)
      } else {
	val modRight = new Mod[U](c.newModId())
	allocations2(key) = modRight
	modRight
      }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (changeable, mod) =
      tbd.TBD.modRightInternal(initializer, modRight, this, key, c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    TBD.nodes(modNode) = (internalId, tag, stack)

    (changeable, mod)
  }
}
