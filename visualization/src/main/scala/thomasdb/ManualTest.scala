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

package thomasdb.visualization

import scala.collection.mutable.ArrayBuffer
import collection.mutable.HashMap
import scala.util.Random
import scala.util.matching.Regex
import scala.io.StdIn

import thomasdb._

/*
 * Test generator which uses user interaction to mutate the input.
 */
class ManualTest[T, V](algorithm: TestAlgorithm[T, V])
    extends TestBase(algorithm) {

  private val putm = "(a) (\\d+) (\\d+)".r
  private val updatem = "(u) (\\d+) (\\d+)".r
  private val remm = "(r) (\\d+)".r
  private val propagatem = "(p)".r
  private val exitm = "(e)".r

  def initialize() {
    println("// Commands: ")
    println("// a KEY VALUE adds a key and value ")
    println("// u KEY VALUE updates a value ")
    println("// r KEY removes a key and value ")
    println("// p propagates ")
    println("// e exits ")
    println("")
    println("// KEY and VALUE have to be integers ")
  }

  def step(): Boolean = {
    var propagate = false
    var continue = true
    while(!propagate) {
      StdIn.readLine() match {
        case putm(_, key, value) => addValue(key.toInt, value.toInt)
        case updatem(_, key, value) => updateValue(key.toInt, value.toInt)
        case remm(_, key) => removeValue(key.toInt)
        case propagatem(_) => propagate = true
        case exitm(_) => propagate = true
                         continue = false
        case _ => println("Invalid Command.")
      }
    }
    continue
  }

  def dispose() = { }
}
