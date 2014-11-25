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
import tbd.ddg._
import tbd.macros.{TbdMacros, functionToInvoke}
import tbd.messages._

class Modizer1[T](implicit c: Context) extends tbd.Modizer1[T] {
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  override def apply(key: Any)
      (initializer: => Changeable[T]): Mod[T] =
    macro TbdMacros.modizerMacro[Mod[T]]

  def applyInternal
      (key: Any,
       initializer: => Changeable[T],
       readerId: Int,
       freeTerms: List[(String, Any)]) = {
    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()

    val mod = super.apply(key)(initializer)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val nodePtr = Timestamp.getNodePtr(c.currentTime)
    TBD.nodes(nodePtr) = (internalId, tag, stack)

    mod
  }
}

class Modizer2[T, U](implicit c: Context) extends tbd.Modizer2[T, U] {
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  override def apply
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U])): (Mod[T], Mod[U]) =
    macro TbdMacros.modizerMacro[(Mod[T], Mod[U])]

  def applyInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       readerId: Int,
       freeTerms: List[(String, Any)]) = {
    if (allocations.contains(key) && c.initialRun) {
      println("WARNING - keyed allocation matched during initial run!")
    }

    if (allocations2.contains(key) && c.initialRun) {
      println("WARNING - keyed allocation matched during initial run!")
    }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (mod1, mod2) = super.apply(key)(initializer)

    val tag = Tag.Mod(List(mod1.id, mod2.id), FunctionTag(readerId, freeTerms))
    val nodePtr = Timestamp.getNodePtr(c.currentTime)
    TBD.nodes(nodePtr) = (internalId, tag, stack)

    (mod1, mod2)
  }

  @functionToInvoke("modLeftInternal")
  override def left(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
        : (Mod[T], Changeable[U]) =
    macro TbdMacros.modizerMacro[(Mod[T], Changeable[U])]

  def modLeftInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Changeable[U]) = {
    if (allocations.contains(key) && c.initialRun) {
      println("WARNING - keyed allocation matched during initial run!")
    }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (mod, changeable) = super.left(key)(initializer)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val nodePtr = Timestamp.getNodePtr(c.currentTime)
    TBD.nodes(nodePtr) = (internalId, tag, stack)

    (mod, changeable)
  }

  @functionToInvoke("modRightInternal")
  override def right(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
        : (Changeable[T], Mod[U]) =
    macro TbdMacros.modizerMacro[(Changeable[T], Mod[U])]

  def modRightInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       readerId: Int,
       freeTerms: List[(String, Any)]): (Changeable[T], Mod[U]) = {
    if (allocations2.contains(key) && c.initialRun) {
      println("WARNING - keyed allocation matched during initial run!")
    }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (changeable, mod) = super.right(key)(initializer)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val nodePtr = Timestamp.getNodePtr(c.currentTime)
    TBD.nodes(nodePtr) = (internalId, tag, stack)

    (changeable, mod)
  }
}
