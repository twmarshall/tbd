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

// Checks that modNoDest returns the correct values even after a convuloted
// series of memo matches.
class ModNoDestTest(input: TableInput[Int, Int]) extends Adjustable {
  val table = input.getTable()
  val one = table.get(1)
  val two = table.get(2)
  val three = table.get(3)
  val four = table.get(4)
  val five = table.get(5)
  val six = table.get(6)
  val seven = table.get(7)

  // If four == 4, twoMemo returns 6, otherwise it returns sevenValue.
  def twoMemo(tbd: TBD, memo: Memoizer[Changeable[Int]]) = {
    tbd.read(four)(fourValue => {
      if (fourValue == 4) {
	tbd.modNoDest(() => {
	  memo(five) {
	    tbd.read(seven)(sevenValue => {
	      tbd.writeNoDest(sevenValue)
	    })
	  }
	})

	memo(six) {
	  tbd.writeNoDest(6)
	}
      } else {
	memo(five) {
	  tbd.read(seven)(sevenValue => {
	    tbd.writeNoDest(sevenValue)
	  })
	}
      }
    })
  }

  def run(tbd: TBD): Mod[Int] = {
    val memo = tbd.makeMemoizer[Changeable[Int]]()

    tbd.modNoDest(() => {
      tbd.read(one)(oneValue => {
	if (oneValue == 1) {
	  val mod1 = tbd.modNoDest(() => {
	    memo(two) {
	      twoMemo(tbd, memo)
	    }
	  })

	  tbd.read(mod1)(value1 => {
	    tbd.writeNoDest(value1)
	  })
	} else {
	  memo(two) {
	    twoMemo(tbd, memo)
	  }
	}
      })
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
    //println(mutator.getDDG())

    input.remove(16)
    input.remove(7)
    //println(mutator.getDDG())
    mutator.propagate()
    //println(mutator.getDDG())

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

  "ModNoDestTest" should "update the dests for the memo matches" in {
    val mutator = new Mutator()
    val input = mutator.createTable[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    input.put(4, 4)
    input.put(5, 5)
    input.put(6, 6)
    input.put(7, 7)
    val output = mutator.run[Mod[Int]](new ModNoDestTest(input))
    output.read() should be (6)

    input.update(1, 2)
    input.update(4, 5)
    mutator.propagate()
    output.read() should be (7)

    input.update(1, 1)
    input.update(7, 8)
    mutator.propagate()
    output.read() should be (8)

    mutator.shutdown()
  }
}
