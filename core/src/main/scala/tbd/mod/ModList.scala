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
package tbd.mod

import akka.actor.ActorRef
import scala.collection.mutable.{Buffer, Set}

import tbd.TBD
import tbd.datastore.Datastore

trait ModList[T] {
  def map[U](tbd: TBD, f: T => U): ModList[U]

  def memoMap[U](tbd: TBD, f: T => U): ModList[U]

  def parMap[U](tbd: TBD, f: (TBD, T) => U): ModList[U]

  def memoParMap[U](tbd: TBD, f: (TBD, T) => U): ModList[U]

  /* Meta functions */
  def toBuffer(): Buffer[T]
}
