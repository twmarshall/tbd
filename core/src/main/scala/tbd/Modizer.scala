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
  val allocations = Map[Any, Dest[Any]]()

  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  def apply(key: Any)(initializer: => Changeable[T])
    (implicit c: Context): Mod[T] = macro TbdMacros.modizerMacro[Mod[T]]

  def applyInternal(
      key: Any,
      initializer: => Changeable[T],
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]) = {
    val oldCurrentDest = c.currentDest

    c.currentDest =
      if (key != null) {
	if (allocations.contains(key)) {
	  allocations(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  allocations(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    val modNode = c.worker.ddg.addMod(c.currentDest.mod, null, c.currentParent,
                                      FunctionTag(readerId, freeTerms))

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.worker.ddg.nextTimestamp(modNode)

    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest

    mod.asInstanceOf[Mod[T]]
  }
}

class Modizer2[T, U] {
  val allocations = Map[Any, Dest[Any]]()
  val allocations2 = Map[Any, Dest[Any]]()

  import scala.language.experimental.macros

  @functionToInvoke("applyInternal")
  def apply
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) =
    macro TbdMacros.modizerMacro[(Mod[T], Mod[U])]

  def applyInternal(
      key: Any,
      initializer: => (Changeable[T], Changeable[U]),
      c: Context,
      readerId: Int,
      freeTerms: List[(String, Any)]) = {
    val oldCurrentDest = c.currentDest

    c.currentDest =
      if (key != null) {
	if (allocations.contains(key)) {
	  if (c.initialRun) {
	    println("WARNING - keyed allocation matched during initial run!")
	  }

	  allocations(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  allocations(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    val oldCurrentDest2 = c.currentDest2

    c.currentDest2 =
      if (key != null) {
	if (allocations2.contains(key)) {
	  if (c.initialRun) {
	    println("WARNING - keyed allocation matched during initial run!")
	  }

	  allocations2(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  allocations2(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    val modNode = c.worker.ddg.addMod(c.currentDest.mod, c.currentDest2.mod,
                                      c.currentParent,
                                      FunctionTag(readerId, freeTerms))

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.worker.ddg.nextTimestamp(modNode)

    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest
    val mod2 = c.currentDest2.mod
    c.currentDest2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
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

    val oldCurrentDest = c.currentDest
    c.currentDest =
      if (key != null) {
	if (allocations.contains(key)) {
	  if (c.initialRun) {
	    println("WARNING - keyed allocation matched during initial run!")
	  }

	  allocations(key)
	} else {
	  val dest = new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  allocations(key) = dest
	  dest
	}
      } else {
	new Dest[T](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    val modNode = c.worker.ddg.addMod(c.currentDest.mod, null,
                                      c.currentParent,
                                      FunctionTag(readerId, freeTerms))
    modNode.currentDest2 = c.currentDest2

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.worker.ddg.nextTimestamp(modNode)

    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest

    (mod.asInstanceOf[Mod[T]],
     new Changeable(c.currentDest2.mod.asInstanceOf[Mod[U]]))
  }

  @functionToInvoke("modRightInternal")
  def right[T, U]
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context):
    (Changeable[T], Mod[U]) = macro TbdMacros.modizerMacro[(Changeable[T], Mod[U])]

  def modRightInternal[T, U]
      (key: Any,
       initializer: => (Changeable[T], Changeable[U]),
       c: Context,
       readerId: Int,
       freeTerms: List[(String, Any)]): (Changeable[T], Mod[U]) = {
    val oldCurrentDest2 = c.currentDest2
    c.currentDest2 =
      if (key != null) {
	if (allocations2.contains(key)) {
	  if (c.initialRun) {
	    println("WARNING - keyed allocation matched during initial run!")
	  }

	  allocations2(key)
	} else {
	  val dest = new Dest[U](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
	  allocations2(key) = dest
	  dest
	}
      } else {
	new Dest[U](c.worker.datastoreRef).asInstanceOf[Dest[Any]]
      }

    val modNode = c.worker.ddg.addMod(null,
                                      c.currentDest2.mod,
                                      c.currentParent,
                                      FunctionTag(readerId, freeTerms))
    modNode.currentDest = c.currentDest

    val outerReader = c.currentParent
    c.currentParent = modNode

    initializer

    c.currentParent = outerReader
    modNode.endTime = c.worker.ddg.nextTimestamp(modNode)

    val mod2 = c.currentDest2.mod
    c.currentDest2 = oldCurrentDest2

    (new Changeable(c.currentDest.mod.asInstanceOf[Mod[T]]),
     mod2.asInstanceOf[Mod[U]])
  }
}
