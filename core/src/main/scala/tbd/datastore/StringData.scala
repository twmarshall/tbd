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
package tbd.datastore

import scala.collection.GenIterable
import scala.collection.mutable.{ArrayBuffer, Map}

import tbd.{Input, Mutator}

class StringData(
    input: Input[Int, String],
    count: Int,
    mutations: List[String],
    check: Boolean
  ) extends Data[String] {

  val maxKey = count * 10

  val rand = new scala.util.Random()

  val file = "data.txt"

  private def loadChunks(
      chunks: ArrayBuffer[String]) {
    val elems = scala.xml.XML.loadFile("wiki.xml")

    var i = 0
    (elems \\ "elem").map(elem => {
      (elem \\ "value").map(value => {
        chunks += value.text
      })
    })
  }

  def generate() {
    while (table.size < count) {
      val elems = scala.xml.XML.loadFile("wiki.xml")

      var i = table.size
      (elems \\ "elem").map(elem => {
        (elem \\ "value").map(value => {
          if (table.size < count) {
            table += (i -> value.text)
            i += 1
          } else {
            chunks += value.text
          }
        })
      })
    }
  }

  val chunks = new ArrayBuffer[String]()
  def load() {
    for (pair <- table) {
      input.put(pair._1, pair._2)
    }
  }

  def clearValues() {}

  def update(n: Int) {
    for (i <- 1 to n) {
      mutations(rand.nextInt(mutations.size)) match {
        case "insert" => addValue()
        case "remove" => removeValue()
        case "update" => updateValue()
      }
    }
  }

  def addValue() {
    if (chunks.size == 0) {
      loadChunks(chunks)
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.put(key, chunks.head)

    table += (key -> chunks.head)

    chunks -= chunks.head
  }

  def removeValue() {
    if (table.size > 1) {
      var key = rand.nextInt(maxKey)
      while (!table.contains(key)) {
        key = rand.nextInt(maxKey)
      }

      input.remove(key, table(key))
      table -= key
    } else {
      addValue()
    }
  }

  def updateValue() {
    if (chunks.size == 0) {
      loadChunks(chunks)
    }

    var key = rand.nextInt(maxKey)
    val value = rand.nextInt(Int.MaxValue)
    while (!table.contains(key)) {
      key = rand.nextInt(maxKey)
    }
    input.update(key, chunks.head)

    table(key) = chunks.head

    chunks -= chunks.head
  }

  def hasUpdates() = true
}
