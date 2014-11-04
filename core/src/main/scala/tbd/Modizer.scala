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
  def remove(key: Any, c: Context): Boolean
}

class Modizer1[T] extends Modizer[T] {
  val allocations = Map[Any, (Mod[T], Int)]()

  def remove(key: Any, c: Context): Boolean = {
    if (allocations(key)._2 < c.epoch) {
      allocations -= key
      true
    } else {
      false
    }
  }

  def apply
      (key: Any)
      (initializer: => Changeable[T])
      (implicit c: Context): Mod[T] = {
    val mod1 =
      if (allocations.contains(key)) {
	allocations(key) = (allocations(key)._1, c.epoch)

	allocations(key)._1
      } else {
	val mod1 = new Mod[T](c.newModId())
	allocations(key) = (mod1, c.epoch)
	mod1
      }

    TBD.modInternal(initializer, mod1, this, key, c)
  }
}

class Modizer2[T, U] extends Modizer[(T, U)] {
  val allocations = Map[Any, (Mod[T], Int)]()
  val allocations2 = Map[Any, (Mod[U], Int)]()

  import scala.language.experimental.macros

  def remove(key: Any, c: Context): Boolean = {
    if (allocations.contains(key)) {
      if (allocations(key)._2 < c.epoch) {
	allocations -= key
	allocations2 -= key

	true
      } else {
	false
      }
    } else {
      if (allocations2(key)._2 < c.epoch) {
	allocations2 -= key

	true
      } else {
	false
      }
    }
  }

  def apply
      (key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Mod[U]) = {
    val modLeft =
      if (allocations.contains(key)) {
	allocations(key) = (allocations(key)._1, c.epoch)

	allocations(key)._1
      } else {
	val modLeft = new Mod[T](c.newModId())
	allocations(key) = (modLeft, c.epoch)
	modLeft
      }

    val modRight =
      if (allocations2.contains(key)) {
	allocations2(key) = (allocations2(key)._1, c.epoch)

	allocations2(key)._1
      } else {
	val modRight = new Mod[U](c.newModId())
	allocations2(key) = (modRight, c.epoch)
	modRight
      }

    TBD.mod2Internal(initializer, modLeft, modRight, this, key, c)
  }

  def left(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Mod[T], Changeable[U]) = {
    val modLeft =
      if (allocations.contains(key)) {
	allocations(key) = (allocations(key)._1, c.epoch)

	allocations(key)._1
      } else {
	val modLeft = new Mod[T](c.newModId())
	allocations(key) = (modLeft, c.epoch)
	modLeft
      }

    TBD.modLeftInternal(initializer, modLeft, this, key, c)
  }

  def right(key: Any)
      (initializer: => (Changeable[T], Changeable[U]))
      (implicit c: Context): (Changeable[T], Mod[U]) = {
    val modRight =
      if (allocations2.contains(key)) {
	allocations2(key) = (allocations2(key)._1, c.epoch)

	allocations2(key)._1
      } else {
	val modRight = new Mod[U](c.newModId())
	allocations2(key) = (modRight, c.epoch)
	modRight
      }

    TBD.modRightInternal(initializer, modRight, this, key, c)
  }
}
