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
package tdb.util

import java.io._
import scala.collection.GenIterable
import scala.collection.mutable.Map

trait Data[Input] {
  val table = Map[Int, Input]()

  val file: String

  private lazy val output = new PrintWriter(new File(file))

  // Fills in 'table' with the initial data set. This must be the first method
  // called on a new Data object.
  def generate()

  // Transfers to data in 'table' into an Input object.
  def load()

  // Updates some values.
  def update(): Int

  // Returns true if this Data can generate more updates.
  def hasUpdates(): Boolean

  protected def log(s: String) {
    output.print(s + "\n")
    output.flush()
  }
}
