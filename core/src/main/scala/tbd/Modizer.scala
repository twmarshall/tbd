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

import scala.collection.mutable.Map

import tbd.ddg.FunctionTag
import tbd.macros.{TbdMacros, functionToInvoke}

class Modizer[T] {
  val allocations = Map[Any, Dest[T]]()

  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  def apply
      (key: Any)
      (initializer: => Changeable[T])
      (implicit c: Context): Mod[T] = macro TbdMacros.modizerMacro[Mod[T]]

  def applyInternal
      (key: Any,
       initializer: => Changeable[T],
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]) = {
    val dest =
      if (allocations.contains(key)) {
	allocations(key)
      } else {
	val dest = new Dest[T](c.worker.datastoreRef)
	allocations(key) = dest
	dest
      }

    TBD.modWithDest(initializer, dest, c, readerId, freeTerms)
  }
}

class Modizer2[T, U] {
  val allocations = Map[Any, Dest[T]]()
  val allocations2 = Map[Any, Dest[U]]()

  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  def apply
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
    val dest1 =
      if (allocations.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations(key)
      } else {
	val dest = new Dest[T](c.worker.datastoreRef)
	allocations(key) = dest
	dest
      }

    val dest2 =
      if (allocations2.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations2(key)
      } else {
	val dest = new Dest[U](c.worker.datastoreRef)
	allocations2(key) = dest
	dest
      }

    TBD.mod2WithDests(initializer, dest1, dest2, c, readerId, freeTerms)
  }

  @functionToInvoke("modLeftInternal")
  def left
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context):
    (Mod[T], Changeable[U]) = macro TbdMacros.modizerMacro[(Mod[T], Changeable[U])]

  def modLeftInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Mod[T], Changeable[U]) = {
    val dest =
      if (allocations.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations(key)
      } else {
	val dest = new Dest[T](c.worker.datastoreRef)
	allocations(key) = dest
	dest
      }

    TBD.modLeftWithDest(initializer, dest, c, readerId, freeTerms)
  }

  @functionToInvoke("modRightInternal")
  def right
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context):
    (Changeable[T], Mod[U]) = macro TbdMacros.modizerMacro[(Changeable[T], Mod[U])]

  def modRightInternal
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Changeable[T], Mod[U]) = {
    val dest =
      if (allocations2.contains(key)) {
	if (c.initialRun) {
	  println("WARNING - keyed allocation matched during initial run!")
	}

	allocations2(key)
      } else {
	val dest = new Dest[U](c.worker.datastoreRef)
	allocations2(key) = dest
	dest
      }

    TBD.modRightWithDest(initializer, dest, c, readerId, freeTerms)
  }
}
