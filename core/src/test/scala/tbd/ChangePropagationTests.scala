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
import scala.collection.mutable.ArrayBuffer

import tbd._
import tbd.mod._

class PropagationOrderTest(input: TableInput[Int, Int]) extends Adjustable {
  var num = 0

  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one)(v1 => {
        assert(num == 0)
        num += 1
        tbd.read(one)(v2 => {
          assert(num == 1)
          num += 1
          tbd.write(dest, v2)
        })
      })

      tbd.read(one)(v3 => {
        assert(num == 2)
        tbd.write(dest, v3)
      })
    })
  }
}

class PropagationOrderTest2(input: ListInput[Int, Int]) extends Adjustable {
  val values = ArrayBuffer[Int]()

  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val adjustableList = input.getAdjustableList()
    adjustableList.map(tbd, (tbd: TBD, pair: (Int, Int)) => {
      if (tbd.initialRun) {
        values += pair._2
      } else {
        assert(pair._2 == values.head + 1)
        values -= values.head
      }
      pair
    })
  }
}

class ReduceTest(input: ListInput[Int, Int]) extends Adjustable {
  def run(tbd: TBD): Mod[(Int, Int)] = {
    val list = input.getAdjustableList()
    val zero = tbd.createMod((0, 0))
    list.reduce(tbd, zero, (tbd: TBD, pair1: (Int, Int), pair2: (Int, Int)) => {
      //println("reducing " + pair1._2 + " " + pair2._2)
      (pair1._1, pair1._2 + pair2._2)
    })
  }
}

class ParTest(input: TableInput[Int, Int]) extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val table = input.getTable()
    val one = table.get(1)

    val pair = tbd.par((tbd: TBD) =>
      tbd.mod((dest: Dest[Int]) =>
        tbd.read(one)(oneValue =>
          tbd.write(dest, oneValue + 1))),
      (tbd: TBD) => 0)

    tbd.mod((dest: Dest[Int]) =>
      tbd.read(pair._1)(value =>
        tbd.write(dest, value * 2)))
  }
}

class MapTest(input: ListInput[Int, Int]) extends Adjustable {
  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val list = input.getAdjustableList()
    list.map(tbd, (tbd: TBD, pair: (Int, Int)) => {
      //println("mapping " + pair)
      pair
    })
  }
}

class ChangePropagationTests extends FlatSpec with Matchers {
  "PropagationOrderTest" should "reexecute reads in the correct order" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    val test = new PropagationOrderTest(input)
    val output = mutator.run[Mod[Int]](test)
    test.num should be (2)

    test.num = 0
    input.update(1, 2)
    mutator.propagate()
    test.num should be (2)
  }

  "PropagationOrderTest2" should "reexecute map in the correct order" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](new ListConf(partitions = 1))

    for (i <- 0 to 100) {
      input.put(i, i)
    }
    val test = new PropagationOrderTest2(input)
    mutator.run[AdjustableList[Int, String]](test)

    for (i <- 0 to 100) {
      input.update(i, i + 1)
    }

    mutator.propagate()

    mutator.shutdown()
  }

  "ReduceTest" should "reexecute only the necessary reduce steps" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](new ListConf(partitions = 1))
    for (i <- 1 to 16) {
      input.put(i, i)
    }
    val test = new ReduceTest(input)
    val output = mutator.run[Mod[(Int, Int)]](test)

    output.read()._2 should be (136)

    input.remove(16)
    input.remove(7)
    mutator.propagate()

    output.read()._2 should be (113)
  }

  "ParTest" should "do something" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    val output = mutator.run[Mod[Int]](new ParTest(input))
    output.read() should be (4)

    input.update(1, 2)
    mutator.propagate()

    output.read() should be (6)

    mutator.shutdown()
  }

  "MapTest" should "do something" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](new ListConf(partitions = 1))
    for (i <- 1 to 5) {
      input.put(i, i)
    }

    val output = mutator.run[AdjustableList[Int, Int]](new MapTest(input))

    //println("propagating")
    input.remove(2)
    mutator.propagate()
  }
}
