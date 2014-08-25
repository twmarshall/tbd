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
package tbd.test

import org.scalatest._

import tbd._
import tbd.list._

class ListTest(input: ListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    input.getAdjustableList()
  }
}

class MutatorTests extends FlatSpec with Matchers {
  val intensity = 10
  def runTest(
      mutator: Mutator,
      adjustable: Adjustable[AdjustableList[Int, Int]],
      input: Input[Int, Int]) {
    val data = new tbd.datastore.IntData(input, intensity, Array("insert", "remove", "update"))
    data.generate()
    data.load()

    val output = mutator.run(adjustable)
    var sortedAnswer = data.table.values.toBuffer.sortWith(_ < _)
    output.toBuffer().map(_._2).sortWith(_ < _) should be (sortedAnswer)

    for (j <- 0 to intensity) {
      data.updateValue()
      sortedAnswer = data.table.values.toBuffer.sortWith(_ < _)
      output.toBuffer().map(_._2).sortWith(_ < _) should be (sortedAnswer)
    }

    for (j <- 0 to intensity) {
      data.addValue()
      sortedAnswer = data.table.values.toBuffer.sortWith(_ < _)
      output.toBuffer().map(_._2).sortWith(_ < _) should be (sortedAnswer)
    }

    while (data.table.size > 0) {
      data.removeValue()
      sortedAnswer = data.table.values.toBuffer.sortWith(_ < _)
      output.toBuffer().map(_._2).sortWith(_ < _) should be (sortedAnswer)
    }

    for (j <- 0 to intensity) {
      data.update()
      sortedAnswer = data.table.values.toBuffer.sortWith(_ < _)
      output.toBuffer().map(_._2).sortWith(_ < _) should be (sortedAnswer)
    }
  }

  "AdjustableListTests" should "update the AdjustableList correctly" in {
    for (partitions <- 1 to 2) {
      for (chunkSize <- 1 to 2) {
	val mutator = new Mutator()

	val conf = new ListConf(partitions = partitions, chunkSize = chunkSize)
	val input = ListInput[Int, Int](conf)
	runTest(mutator, new ListTest(input), input)

	mutator.shutdown()
      }
    }
  }
}
