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
package tbd.datastore

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Future

class ModModifier[T, U](_datastore: Datastore, _key: T, value: U)
    extends Modifier[T, U](_datastore) {
  val mod = datastore.createMod(value)

  def insert(key: T, value: U): ArrayBuffer[Future[String]] = ???

  def update(key: T, value: U): ArrayBuffer[Future[String]] = {
    if (key == _key) {
      datastore.updateMod(mod.id, value)
    } else {
      ArrayBuffer[Future[String]]()
    }
  }

  def remove(key: T): ArrayBuffer[Future[String]] = ???

  def getModifiable(): Any = mod
}
