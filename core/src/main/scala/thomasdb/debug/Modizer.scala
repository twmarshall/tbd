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
package thomasdb.debug

import akka.pattern.ask
import scala.collection.mutable.Map
import scala.concurrent.Await

import thomasdb._
import thomasdb.Constants._
import thomasdb.ddg.{FunctionTag, ModNode, Node, Tag}
import thomasdb.macros.{ThomasDBMacros, functionToInvoke}
import thomasdb.messages._

class Modizer1[T] extends thomasdb.Modizer1[T] {
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  override def apply(key: Any)
      (initializer: => Changeable[T])
      (implicit c: Context): Mod[T] = macro ThomasDBMacros.modizerMacro[Mod[T]]

  def applyInternal
      (key: Any,
       initializer: => Changeable[T],
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]) = {
    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()

    val mod = super.apply(key)(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    ThomasDB.nodes(modNode) = (internalId, tag, stack)

    mod
  }
}

class Modizer2[T, U] extends thomasdb.Modizer2[T, U] {
  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  override def apply
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) =
    macro ThomasDBMacros.modizerMacro[(Mod[T], Mod[U])]

  def applyInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
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
    val (mod1, mod2) =
      super.apply(key)(initializer)(c)

    val tag = Tag.Mod(List(mod1.id, mod2.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    ThomasDB.nodes(modNode) = (internalId, tag, stack)

    (mod1, mod2)
  }

  @functionToInvoke("modLeftInternal")
  override def left(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) =
    macro ThomasDBMacros.modizerMacro[(Mod[T], Changeable[U])]

  def modLeftInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Changeable[U]) = {
    if (allocations.contains(key) && c.initialRun) {
      println("WARNING - keyed allocation matched during initial run!")
    }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (mod, changeable) =
      super.left(key)(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    ThomasDB.nodes(modNode) = (internalId, tag, stack)

    (mod, changeable)
  }

  @functionToInvoke("modRightInternal")
  override def right(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) =
    macro ThomasDBMacros.modizerMacro[(Changeable[T], Mod[U])]

  def modRightInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Changeable[T], Mod[U]) = {
    if (allocations2.contains(key) && c.initialRun) {
      println("WARNING - keyed allocation matched during initial run!")
    }

    val internalId = Node.getId()
    val stack = Thread.currentThread().getStackTrace()
    val (changeable, mod) =
      super.right(key)(initializer)(c)

    val tag = Tag.Mod(List(mod.id), FunctionTag(readerId, freeTerms))
    val modNode = c.currentTime.node
    ThomasDB.nodes(modNode) = (internalId, tag, stack)

    (changeable, mod)
  }
}
