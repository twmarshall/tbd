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
import scala.collection.mutable.Map
import scala.concurrent.Await

import tbd.Constants._
import tbd.ddg.{FunctionTag, ModNode, Tag}
import tbd.macros.{TbdMacros, functionToInvoke}
import tbd.messages._

trait Modizer[T] {
  def remove(key: Any)
}

class Modizer1[T] extends Modizer[T] {
  val allocations = Map[Any, Mod[T]]()

  def remove(key: Any) {
    allocations -= key
  }

  def apply
      (key: Any)
      (initializer: => Changeable[T])
      (implicit c: Context): Mod[T] = {
    val mod1 =
      if (allocations.contains(key)) {
	allocations(key)
      } else {
	val mod1 = new Mod[T](c.newModId())
	allocations(key) = mod1
	mod1
      }

    TBD.modInternal(initializer, mod1, this, key, c)
  }
}

class Modizer2[T, U] extends Modizer[(T, U)] {
  val allocations = Map[Any, Mod[T]]()
  val allocations2 = Map[Any, Mod[U]]()

  import scala.language.experimental.macros

  def remove(key: Any) {
    allocations -= key
    allocations2 -= key
  }

  def apply
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) = {
    val modLeft =
      if (allocations.contains(key)) {
	allocations(key)
      } else {
	val modLeft = new Mod[T](c.newModId())
	allocations(key) = modLeft
	modLeft
      }

    val modRight =
      if (allocations2.contains(key)) {
	allocations2(key)
      } else {
	val modRight = new Mod[U](c.newModId())
	allocations2(key) = modRight
	modRight
      }

    TBD.mod2Internal(initializer, modLeft, modRight, this, key, c)
  }

  def left(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) = {
    val modLeft =
      if (allocations.contains(key)) {
	allocations(key)
      } else {
	val modLeft = new Mod[T](c.newModId())
	allocations(key) = modLeft
	modLeft
      }

    TBD.modLeftInternal(initializer, modLeft, this, key, c)
  }

  def right(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) = {
    val modRight =
      if (allocations2.contains(key)) {
	allocations2(key)
      } else {
	val modRight = new Mod[U](c.newModId())
	allocations2(key) = modRight
	modRight
      }

    TBD.modRightInternal(initializer, modRight, this, key, c)
  }
}
