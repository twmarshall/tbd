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
package tbd.util

import java.io._
import scala.collection.mutable.Buffer

import tbd.Input

class IntFileData
    (input: Input[Int, Int],
     fileName: String) extends Data[Int] {
  var lines = io.Source.fromFile(fileName).getLines().toBuffer

  val file = "data.txt"

  def generate() {
    while (lines.size > 0 && lines.head.startsWith("generated")) {
      val split = lines.head.drop(11).dropRight(1).split(",")
      assert(split.size == 2)
      table += ((split(0).toInt, split(1).toInt))
      lines = lines.tail
    }
  }

  def load() {
    for (pair <- table) {
      input.put(pair._1, pair._2)
    }
  }

  def update() {
    while (lines.head != "---") {
      val space = lines.head.indexOf(" ")
      val (command, pair) = lines.head.splitAt(space)

      val split = pair.drop(2).dropRight(1).split(",")
      val key = split(0).toInt
      val value = split(1).toInt

      command match {
        case "adding" =>
          input.put(key, value)
          table(key) = value
        case "removing" =>
          input.remove(key, table(key))
          table -= key
        case "updating" =>
          input.update(key, value)
          table(key) = value
      }

      lines = lines.tail
    }

    lines = lines.tail
  }

  def hasUpdates() = lines.size > 0
}
