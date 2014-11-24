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
import tbd.messages._

trait Modizer[T] {
  def remove(key: Any): Boolean
}

class Modizer1[T](implicit c: Context) extends Modizer[T] {
  val allocations = Map[Any, (Mod[T], Int)]()

  val id = c.newModizerId(this)

  def remove(key: Any): Boolean = {
    if (allocations(key)._2 < c.epoch) {
      allocations -= key
      true
    } else {
      false
    }
  }

  def apply
      (key: Any)
      (initializer: => Changeable[T]): Mod[T] = {
    val mod1 =
      if (allocations.contains(key)) {
        allocations(key) = (allocations(key)._1, c.epoch)

        allocations(key)._1
      } else {
        val mod1 = new Mod[T](c.newModId())
        allocations(key) = (mod1, c.epoch)
        mod1
      }

    TBD.modizerHelper(initializer, mod1.id, -1, id, key, -1, -1, c)

    mod1
  }
}

class Modizer2[T, U](implicit c: Context) extends Modizer[(T, U)] {
  val allocations = Map[Any, (Mod[T], Int)]()
  val allocations2 = Map[Any, (Mod[U], Int)]()

  val id = c.newModizerId(this)

  def remove(key: Any): Boolean = {
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
      (initializer: => (Changeable[T], Changeable[U])): (Mod[T], Mod[U]) = {
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

    TBD.modizerHelper(initializer, modLeft.id, modRight.id, id, key, -1, -1, c)

    (modLeft, modRight)
  }

  def left(key: Any)
      (initializer: => (Changeable[T], Changeable[U])): (Mod[T], Changeable[U]) = {
    val modLeft =
      if (allocations.contains(key)) {
        allocations(key) = (allocations(key)._1, c.epoch)

        allocations(key)._1
      } else {
        val modLeft = new Mod[T](c.newModId())
        allocations(key) = (modLeft, c.epoch)
        modLeft
      }

    TBD.modizerHelper(initializer, modLeft.id, -1, id, key, -1, c.currentModId2, c)

    (modLeft, new Changeable[U](c.currentModId2))
  }

  def right(key: Any)
      (initializer: => (Changeable[T], Changeable[U])): (Changeable[T], Mod[U]) = {
    val modRight =
      if (allocations2.contains(key)) {
        allocations2(key) = (allocations2(key)._1, c.epoch)

        allocations2(key)._1
      } else {
        val modRight = new Mod[U](c.newModId())
        allocations2(key) = (modRight, c.epoch)
        modRight
      }

    TBD.modizerHelper(initializer, -1, modRight.id, id, key, c.currentModId, -1, c)

    (new Changeable[T](c.currentModId), modRight)
  }
}
