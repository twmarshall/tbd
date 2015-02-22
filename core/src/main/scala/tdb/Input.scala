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
package tdb

import scala.concurrent.Future

trait Input[T, U] {
  def put(key: T, value: U)

  def asyncPut(key: T, value: U): Future[_]

  def asyncPutAll(values: Iterable[(T, U)]): Future[_]

  def remove(key: T, value: U)

  def removeAll(values: Iterable[(T, U)])

  def asyncRemove(key: T, value: U): Future[_]

  def flush(): Unit = ???
}
