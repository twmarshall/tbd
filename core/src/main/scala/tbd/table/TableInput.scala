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
package tbd.table

import tbd.{Input, Mutator}

object TableInput {
  def apply[T, U](mutator: Mutator) = new TableInput[T, U](mutator)
}

class TableInput[T, U](mutator: Mutator) extends Input[T, U] {
  val table = new ModTable[T, U]()

  def put(key: T, value: U) {
    table.table(key) = mutator.createMod(value)
  }

  def update(key: T, value: U) {
    mutator.updateMod(table.table(key), value)
  }

  def remove(key: T) = ???

  def getTable(): ModTable[T, U] = table
}
