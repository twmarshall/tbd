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
package tdb.datastore

import scala.collection.mutable.Buffer
import scala.concurrent.Future

import tdb.Constants._
import tdb.list.AdjustableList
import tdb.Mod

trait Modifier {
  def loadInput(keys: Iterable[Any]): Future[_]

  def put(key: Any, value: Any): Future[_]

  def putIn(column: String, key: Any, value: Any): Future[_]

  def get(key: Any): Any

  def remove(key: Any, value: Any): Future[_]

  def getAdjustableList(): AdjustableList[Any, Any]

  def toBuffer(): Buffer[(Any, Any)]
}
