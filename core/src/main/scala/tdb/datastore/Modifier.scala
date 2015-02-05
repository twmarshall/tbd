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
package tdb.list

import scala.collection.mutable.Map
import scala.concurrent.Future

import tdb.Constants._
import tdb.Mod

trait Modifier {
  // Inserts all of the elements from data into this ListInput. Assumes that
  // the list is currently empty.
  def load(data: Map[Any, Any]): Future[_]

  def loadInput(keys: Iterable[Int]): Future[_]

  def asyncPut(key: Any, value: Any): Future[_]

  def update(key: Any, value: Any): Future[_]

  def remove(key: Any, value: Any): Future[_]

  def getAdjustableList(): AdjustableList[Any, Any]
}
