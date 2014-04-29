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
package tbd.examples.list

import scala.collection.mutable.Map

import tbd.{Adjustable, Mutator, TBD}
import tbd.mod.ModList

object MapAdjust {
  def mapper(s: String): Int = {
    var count = 0
    for (word <- s.split("\\W+")) {
      count += 1
    }
    count
  }
}

class MapAdjust(partitions: Int) extends Algorithm {
  var output: ModList[Int] = null

  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.map(tbd, (s: String) => MapAdjust.mapper(s))
  }

  def initialRun(mutator: Mutator) {
    output = mutator.run[ModList[Int]](this)
  }

  def checkOutput(answer: Map[Int, String]): Boolean = {
    val sortedOutput = output.toBuffer().sortWith(_ < _)
    val sortedAnswer = answer.map(pair => {
      MapAdjust.mapper(pair._2)
    }).toBuffer.sortWith(_ < _)

    return sortedOutput == sortedAnswer
  }
}

class MapMemoAdjust(partitions: Int) extends Algorithm {
  var output: ModList[Int] = null

  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.memoMap(tbd, (s: String) => MapAdjust.mapper(s))
  }

  def initialRun(mutator: Mutator) {
    output = mutator.run[ModList[Int]](this)
  }

  def checkOutput(answer: Map[Int, String]): Boolean = {
    val sortedOutput = output.toBuffer().sortWith(_ < _)
    val sortedAnswer = answer.map(pair => {
      MapAdjust.mapper(pair._2)
    }).toBuffer.sortWith(_ < _)

    return sortedOutput == sortedAnswer
  }
}

class MapParAdjust(partitions: Int) extends Algorithm {
  var output: ModList[Int] = null

  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.parMap(tbd, (tbd: TBD, s: String) => MapAdjust.mapper(s))
  }

  def initialRun(mutator: Mutator) {
    output = mutator.run[ModList[Int]](this)
  }

  def checkOutput(answer: Map[Int, String]): Boolean = {
    val sortedOutput = output.toBuffer().sortWith(_ < _)
    val sortedAnswer = answer.map(pair => {
      MapAdjust.mapper(pair._2)
    }).toBuffer.sortWith(_ < _)

    return sortedOutput == sortedAnswer
  }
}

class MapMemoParAdjust(partitions: Int) extends Algorithm {
  var output: ModList[Int] = null

  def run(tbd: TBD): ModList[Int] = {
    val pages = tbd.input.getModList[String](partitions)
    pages.memoParMap(tbd, (tbd: TBD, s: String) => MapAdjust.mapper(s))
  }

  def initialRun(mutator: Mutator) {
    output = mutator.run[ModList[Int]](this)
  }

  def checkOutput(answer: Map[Int, String]): Boolean = {
    val sortedOutput = output.toBuffer().sortWith(_ < _)
    val sortedAnswer = answer.map(pair => {
      MapAdjust.mapper(pair._2)
    }).toBuffer.sortWith(_ < _)

    return sortedOutput == sortedAnswer
  }
}
