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

import tbd.{Adjustable, Changeable, Dest, Mutator, ListNode, TBD}
import tbd.datastore.Dataset
import tbd.mod.Mod

class ListMapReduceTest extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val dataset = tbd.input.getDataset[Int](partitions = 1)
    val mappedDataset = dataset.map(tbd, (_: Int) * 2)
    mappedDataset.reduce(tbd, (_: Int) + (_: Int))
  }
}

class ListParMapReduceTest extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val dataset = tbd.input.getDataset[Int]()
    val mappedDataset = dataset.parMap(tbd, (_: Int) + 1)
    mappedDataset.parReduce(tbd, (_: Int) + (_: Int))
  }
}

class ListMemoMapTest extends Adjustable {
  def run(tbd: TBD): Dataset[Int] = {
    val dataset = tbd.input.getDataset[Int](partitions = 1)
    dataset.memoMap(tbd, (_: Int) + 3)
  }
}

class ListTests extends FlatSpec with Matchers {
  "ListMapReduceTest" should "return the reduced value" in {
    val mutator = new Mutator()
    mutator.put("one", 1)
    mutator.put("two", 2)
    val output = mutator.run[Mod[Int]](new ListMapReduceTest())
    // (1 * 2) + (2 * 2)
    output.read() should be (6)

    mutator.update("one", 5)
    mutator.propagate()
    // (5 * 2) + (2 * 2)
    output.read() should be (14)

    mutator.put("three", 4)
    mutator.propagate()
    // (5 * 2) + (2 * 2) + (4 * 2)
    output.read() should be (22)

    mutator.update("three", 3)
    mutator.update("one", -2)
    mutator.put("four", 6)
    mutator.propagate()
    // (-2 * 2) + (2 * 2) + (3 * 2) + (6 * 2)
    output.read() should be (18)

    mutator.put("five", 9)
    mutator.update("five", 8)
    mutator.put("six", 10)
    mutator.update("four", 3)
    mutator.put("seven", 5)
    mutator.propagate()
    // (-2 * 2) + (2 * 2) + (3 * 2) + (3 * 2) + (8 * 2) + (10 * 2) + (5 * 2)
    output.read() should be (58)

    mutator.shutdown()
  }

  "ListParMapReduceTest" should "return the reduced value" in {
    val mutator = new Mutator()
    mutator.put("one", 1)
    mutator.put("two", 2)
    val output = mutator.run[Mod[Int]](new ListParMapReduceTest())
    // (1 + 1) + (2 + 1)
    output.read() should be (5)

    mutator.put("three", 3)
    mutator.propagate()
    // (1 + 1) + (2 + 1) + (3 + 1)
    output.read() should be (9)

    mutator.update("one", 4)
    mutator.propagate()
    // (4 + 1) + (2 + 1) + (3 + 1)
    output.read() should be (12)

    mutator.update("three", 2)
    mutator.update("one", 7)
    mutator.propagate()
    // (7 + 1) + (2 + 1) + (2 + 1)
    output.read() should be (14)

    mutator.put("four", -1)
    mutator.put("five", 10)
    mutator.propagate()
    // (7 + 1) + (2 + 1) + (2 + 1) + (-1 + 1) + (10 + 1)
    output.read() should be (25)

    mutator.put("six", -3)
    mutator.update("four", 3)
    mutator.update("three", 5)
    mutator.propagate()
    // (7 + 1) + (2 + 1) + (5 + 1) + (3 + 1) + (10 + 1) + (-3 + 1)
    output.read() should be (30)

    mutator.shutdown()
  }

  "ListMemoMapTest" should "return the mapped Dataset" in {
    val mutator = new Mutator()
    mutator.put("one", 1)
    mutator.put("two", 2)
    mutator.put("three", 3)
    mutator.put("four", 4)
    val output = mutator.run[Dataset[Int]](new ListMemoMapTest())
    output.toSet() should be (Set(4, 5, 6, 7))

    mutator.remove("two")
    mutator.propagate()
    output.toSet() should be (Set(4, 6, 7))

    mutator.put("five", 5)
    mutator.remove("three")
    mutator.propagate()
    output.toSet() should be (Set(4, 7, 8))

    mutator.put("six", 6)
    mutator.put("seven", 7)
    mutator.put("eight", 8)
    mutator.remove("six")
    mutator.propagate()
    output.toSet() should be (Set(4, 7, 8, 10, 11))

    mutator.remove("one")
    mutator.remove("five")
    mutator.remove("eight")
    mutator.propagate()
    output.toSet() should be (Set(7, 10))

    mutator.update("four", -4)
    mutator.put("nine", 9)

    mutator.remove("seven")
    mutator.propagate()
    output.toSet() should be (Set(-1, 12))
  }
}
