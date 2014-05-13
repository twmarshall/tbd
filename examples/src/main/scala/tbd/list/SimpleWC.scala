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

import java.io.{BufferedInputStream, File, FileInputStream}

import scala.collection.mutable.{ArrayBuffer, Map}

class SimpleWC(
    inputSize: Int,
    partitions: Int,
    parallel: Boolean) extends ControlAlgorithm {
  val chunkSize = inputSize / partitions

  val chunks = ArrayBuffer[String]()
  def loadFile(chunkSize: Int) {
    val bb = new Array[Byte](chunkSize)
    val bis = new BufferedInputStream(new FileInputStream(new File("input.txt")))
    var bytesRead = bis.read(bb, 0, chunkSize)

    while (bytesRead > 0) {
      chunks += new String(bb)
      bytesRead = bis.read(bb, 0, chunkSize)
    }

    bis.close()
  }

  def run(): Long = {
    def generate(i: Int): Vector[String] = {
      if (i == partitions) {
        Vector[String]()
      } else {
        if (chunks.size == 0) {
          loadFile(chunkSize)
        }

        val elem = chunks.head
        chunks -= elem
        generate(i + 1) :+ elem
      }
    }

    val vector =
      if (parallel) {
        generate(0).par
      } else {
        generate(0)
      }

    val before = System.currentTimeMillis()
    vector.aggregate(Map[String, Int]())((x, line) => WC.countReduce(line, x), WC.reduce)
    System.currentTimeMillis() - before
  }
}
