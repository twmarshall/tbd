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

import tbd.TBD

/**
 * A simple iterator trait.
 */
trait Iterable[T, U, N <: Iterator[T, U, N]] {
  /**
   * Returns multiple disjoint iterators for the collection, which can be used
   * in parallel. It is guaranteed that at least one iterator is returned.
   */
  def iterators(tbd: TBD): List[Mod[N]]
}
