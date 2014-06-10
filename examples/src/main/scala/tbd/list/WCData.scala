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

import scala.collection.GenIterable
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.{Input, Mutator}

class WCData(input: Input[Int, String], count: Int, mutations: Array[String])
    extends Data[String] {
  val maxKey = count * 10

  val chunks = ArrayBuffer[String]()
  val table = Map[Int, String]()

  val rand = new scala.util.Random()

  {
    var i = 0
    while (table.size < count) {
      if (chunks.size == 0) {
        chunks ++= loadPages()
      }

      table += (i -> chunks.head)
      chunks -= chunks.head
      i += 1
    }
  }

  private def loadPages(): ArrayBuffer[String] = {
    val chunks = ArrayBuffer[String]()
    val elems = scala.xml.XML.loadFile("wiki.xml")

    (elems \\ "elem").map(elem => {
      (elem \\ "value").map(value => {
	chunks += value.text
      })
    })

    chunks
  }

  def prepareNaive(parallel: Boolean): GenIterable[String] =
    if(parallel)
      Vector[String](table.values.toSeq: _*).par
    else
      Vector[String](table.values.toSeq: _*)

  def loadInitial() {
    for (pair <- table) {
      input.put(pair._1, pair._2)

      if (!Experiment.check) {
        table(pair._1) = ""
      }
    }
  }

  def prepareCheck(): GenIterable[String] =
    table.values.par

  def update() {
    mutations(rand.nextInt(mutations.size)) match {
      case "insert" => addValue()
      case "remove" => removeValue()
      case "update" => updateValue()
    }
  }

  def addValue() {
    if (chunks.size == 0) {
      chunks ++= loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key, chunks.head)

    if (Experiment.check) {
      table += (key -> chunks.head)
    } else {
      table += (key -> "")
    }

    chunks -= chunks.head
  }

  def removeValue() {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }
      input.remove(key)
      table -= key
    } else {
      addValue()
    }
  }

  def updateValue() {
    if (chunks.size == 0) {
      chunks ++= loadPages()
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.update(key, chunks.head)

    if (Experiment.check) {
      table(key) = chunks.head
    }

    chunks -= chunks.head
  }
}
