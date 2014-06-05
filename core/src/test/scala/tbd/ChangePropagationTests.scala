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

import tbd.{Adjustable, Changeable, Mutator, TBD}
import tbd.mod.{Dest, Mod, AdjustableList}

class PropagationOrderTest extends Adjustable {
  var num = 0

  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.getMod[Int](1)

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

class PropagationOrderTest2 extends Adjustable {
  val values = ArrayBuffer[Int]()

  def run(tbd: TBD): AdjustableList[Int, Int] = {
    val adjustableList = tbd.input.getAdjustableList[Int, Int](partitions = 1)
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

class ReduceTest extends Adjustable {
  def run(tbd: TBD): Mod[(Int, Int)] = {
    val list = tbd.input.getAdjustableList[Int, Int](partitions = 1)
    val zero = tbd.createMod((0, 0))
    list.reduce(tbd, zero, (tbd: TBD, pair1: (Int, Int), pair2: (Int, Int)) => {
      //println("reducing " + pair1._2 + " " + pair2._2)
      (pair1._1, pair1._2 + pair2._2)
    })
  }
}

class ParTest extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.getMod[Int](1)

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

class AsyncTest extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.getMod[Int](1)

    var isAsync = true

    val two = tbd.asyncMod((tbd: TBD, dest: Dest[Int]) => {
        tbd.read(one)(one => {
          println("Async execution")
          Thread.sleep(1000)
          isAsync = false
          tbd.write(dest, one + 1)
        })
    })

    Thread.sleep(200)

    assert(isAsync)

    val three = tbd.mod((dest: Dest[Int]) => {
        tbd.read(two)(two => {
            println("Async result read")
           tbd.write(dest, two + 1)
        })
    })

    three
  }
}

class ChangePropagationTests extends FlatSpec with Matchers {
  /*"PropagationOrderTest" should "reexecute reads in the correct order" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    val test = new PropagationOrderTest()
    val output = mutator.run[Mod[Int]](test)
    test.num should be (2)

    test.num = 0
    mutator.update(1, 2)
    mutator.propagate()
    test.num should be (2)
  }

  "PropagationOrderTest2" should "reexecute map in the correct order" in {
    val mutator = new Mutator()
    for (i <- 0 to 100) {
      mutator.put(i, i)
    }
    val test = new PropagationOrderTest2()
    mutator.run[AdjustableList[Int, String]](test)

    for (i <- 0 to 100) {
      mutator.update(i, i + 1)
    }

    mutator.propagate()

    mutator.shutdown()
  }

  "ReduceTest" should "reexecute only the necessary reduce steps" in {
    val mutator = new Mutator()
    for (i <- 1 to 16) {
      mutator.put(i, i)
    }
    val test = new ReduceTest()
    val output = mutator.run[Mod[(Int, Int)]](test)

    output.read()._2 should be (136)
    //println(mutator.getDDG())

    mutator.remove(16)
    mutator.remove(7)
    //println(mutator.getDDG())
    mutator.propagate()
    //println(mutator.getDDG())

    output.read()._2 should be (113)
  }

  "ParTest" should "do something" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    val output = mutator.run[Mod[Int]](new ParTest())
    output.read() should be (4)

    mutator.update(1, 2)
    mutator.propagate()

    output.read() should be (6)
  }
*/
  "AsyncTest" should "Should add numbers asynchronously and safe" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    val output = mutator.run[Mod[Int]](new AsyncTest())
    output.read() should be (3)
    println("Result 1: " + output.read())
    mutator.update(1, 2)
    mutator.propagate()

    output.read() should be (4)
    println("Result 2: " + output.read())
  }
}
