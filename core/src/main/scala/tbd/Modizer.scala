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

class Modizer[T] {
  val allocations = Map[Any, Dest[Any]]()

  def apply(key: Any)(initializer: => Changeable[T])(implicit c: Context) = {
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

    initializer
    val mod = c.currentDest.mod
    c.currentDest = oldCurrentDest

    mod.asInstanceOf[Mod[T]]
  }
}
