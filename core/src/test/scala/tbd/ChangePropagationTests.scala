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
import tbd.list._
import tbd.table._
import tbd.TBD._

class PropagationOrderTest(input: TableInput[Int, Int])
    extends Adjustable[Mod[Int]] {
  var num = 0

  def run(implicit c: Context) = {
    val table = input.getTable()
    val one = table.get(1)

    mod {
      read(one) {
	case v1 =>
          assert(num == 0)
          num += 1
          read(one) {
	    case v2 =>
              assert(num == 1)
              num += 1
              write(v2)
          }
      }

      read(one) {
	  case v3 =>
          assert(num == 2)
          write(v3)
      }
    }
  }
}

class PropagationOrderTest2(input: ListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {
  val values = ArrayBuffer[Int]()

  def run(implicit c: Context) = {
    val adjustableList = input.getAdjustableList()
    adjustableList.map((pair: (Int, Int)) => {
      if (c.initialRun) {
        values += pair._2
      } else {
        assert(pair._2 == values.head + 1)
        values -= values.head
      }
      pair
    })
  }
}

class ReduceTest(input: ListInput[Int, Int])
    extends Adjustable[Mod[(Int, Int)]] {
  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    list.reduce((pair1: (Int, Int), pair2: (Int, Int)) => {
      //println("reducing " + pair1._2 + " " + pair2._2)
      (pair1._1, pair1._2 + pair2._2)
    })
  }
}

class ParTest(input: TableInput[Int, Int]) extends Adjustable[Mod[Int]] {
  def run(implicit c: Context) = {
    val table = input.getTable()
    val one = table.get(1)

    val pair = par {
      c =>
	mod {
          read(one) {
            case oneValue => write(oneValue + 1)(c)
	  } (c)
	} (c)
    } and {
      c => 0
    }

    mod {
      read(pair._1) {
	case value => write(value * 2)
      }
    }
  }
}

// Checks that modNoDest returns the correct values even after a convuloted
// series of memo matches.
class ModNoDestTest(input: TableInput[Int, Int])
    extends Adjustable[Mod[Int]] {
  val table = input.getTable()
  val one = table.get(1)
  val two = table.get(2)
  val three = table.get(3)
  val four = table.get(4)
  val five = table.get(5)
  val six = table.get(6)
  val seven = table.get(7)

  // If four == 4, twoMemo returns 6, otherwise it returns sevenValue.
  def twoMemo(memo: Memoizer[Changeable[Int]])(implicit c: Context) = {
    read(four)(fourValue => {
      if (fourValue == 4) {
	mod {
	  memo(five) {
	    read(seven)(sevenValue => {
	      write(sevenValue)
	    })
	  }
	}

	memo(six) {
	  write(6)
	}
      } else {
	memo(five) {
	  read(seven)(sevenValue => {
	    write(sevenValue)
	  })
	}
      }
    })
  }

  def run(implicit c: Context) = {
    val memo = makeMemoizer[Changeable[Int]]()

    mod {
      read(one)(oneValue => {
	if (oneValue == 1) {
	  val mod1 = mod {
	    memo(two) {
	      twoMemo(memo)
	    }
	  }

	  read(mod1)(value1 => {
	    write(value1)
	  })
	} else {
	  memo(two) {
	    twoMemo(memo)
	  }
	}
      })
    }
  }
}

class SortTest(input: ListInput[Int, Double])
    extends Adjustable[AdjustableList[Int, Double]] {
  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    list.sort((pair1, pair2) => {
      println("      comparing " + pair1 + " " + pair2)
      pair1._2 < pair2._2
    })
  }
}

class ChunkSortTest(input: ChunkListInput[Int, Double])
    extends Adjustable[AdjustableList[Int, Double]] {
  def run(implicit c: Context) = {
    val list = input.getChunkList()
    list.sort((pair1, pair2) => {
      println("      comparing " + pair1 + " " + pair2)
      pair1._2 < pair2._2
    })
  }
}

class SplitTest(input: ListInput[Int, Int], input2: TableInput[Int, Int])
    extends Adjustable[Mod[(AdjustableList[Int, Int], AdjustableList[Int, Int])]] {
  def run(implicit c: Context) = {
    val table = input2.getTable()
    val pivot = table.get(1)

    val list = input.getAdjustableList()

    mod {
      read(pivot) {
	case pivot =>
	  write(list.split(pair => {
	    println("splitting " + pair)
	    pair._2 < pivot
	  }))
      }
    }
  }
}

class MergeTest(input: ListInput[Int, Int], input2: ListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    val list = input.getAdjustableList()
    val list2 = input2.getAdjustableList()

    list.asInstanceOf[ModList[Int, Int]]
      .merge(list2.asInstanceOf[ModList[Int, Int]], (pair: (Int, Int), pair2: (Int, Int)) => {
	println("comparing " + pair + " " + pair2)
	pair._2 <= pair2._2
      })
  }
}

class ChunkMergeTest(input: ChunkListInput[Int, Int], input2: ChunkListInput[Int, Int])
    extends Adjustable[AdjustableList[Int, Int]] {
  def run(implicit c: Context) = {
    val list = input.getChunkList()
    val list2 = input2.getChunkList()

    list.asInstanceOf[ChunkList[Int, Int]]
      .merge(list2.asInstanceOf[ChunkList[Int, Int]], (pair: (Int, Int), pair2: (Int, Int)) => {
	println("comparing " + pair + " " + pair2)
	pair._2 <= pair2._2
      })
  }
}

class MapReduceTest(input: ListInput[Int, Int])
    extends Adjustable[Mod[(Int, Int)]] {
  def run(implicit c: Context) = {
    val list = input.getAdjustableList()

    val mapped = list.map(pair => {
      println("mapping " + pair)
      (pair._1, pair._2 * 2)
    })

    mapped.reduce((pair1, pair2) => {
      println("reducing " + pair1 + " and " + pair2)
      (pair1._1, pair1._2 + pair2._2)
    })
  }
}

class ChangePropagationTests extends FlatSpec with Matchers {
  /*"PropagationOrderTest" should "reexecute reads in the correct order" in {
    val mutator = new Mutator()
    val input = TableInput[Int, Int]()
    input.put(1, 1)
    val test = new PropagationOrderTest(input)
    val output = mutator.run(test)
    test.num should be (2)

    test.num = 0
    input.update(1, 2)
    mutator.propagate()
    test.num should be (2)
  }

  "PropagationOrderTest2" should "reexecute map in the correct order" in {
    val mutator = new Mutator()
    val input = ListInput[Int, Int](new ListConf(partitions = 1))

    for (i <- 0 to 100) {
      input.put(i, i)
    }
    val test = new PropagationOrderTest2(input)
    mutator.run(test)

    for (i <- 0 to 100) {
      input.update(i, i + 1)
    }

    mutator.propagate()

    mutator.shutdown()
  }*/

  /*"ReduceTest" should "reexecute only the necessary reduce steps" in {
    val mutator = new Mutator()
    val input = ListInput[Int, Int](new ListConf(partitions = 1))
    for (i <- 1 to 16) {
      input.put(i, i)
    }
    val test = new ReduceTest(input)
    val output = mutator.run(test)

    output.read()._2 should be (136)
    input.remove(16)
    input.remove(7)
    mutator.propagate()

    output.read()._2 should be (113)
  }*/

  /*"ParTest" should "do something" in {
    val mutator = new Mutator()
    val input = TableInput[Int, Int]()
    input.put(1, 1)
    val output = mutator.run(new ParTest(input))
    output.read() should be (4)

    input.update(1, 2)
    mutator.propagate()

    output.read() should be (6)

    mutator.shutdown()
  }

  "ModNoDestTest" should "update the dests for the memo matches" in {
    val mutator = new Mutator()
    val input = TableInput[Int, Int]()
    input.put(1, 1)
    input.put(2, 2)
    input.put(3, 3)
    input.put(4, 4)
    input.put(5, 5)
    input.put(6, 6)
    input.put(7, 7)
    val output = mutator.run(new ModNoDestTest(input))
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
  }*/

  /*"SortTest" should "only reexecute the least possible" in {
    val mutator = new Mutator()
    val input = ListInput[Int, Double](new ListConf(chunkSize = 1, partitions = 1))
    for (i <- List(10, 5, 6, 1, 7, 4, 8, 3, 2, 9)) {
      input.put(i, i)
    }
    val output = mutator.run(new SortTest(input))
    println(output.toBuffer)

    println("\npropagating")
    input.update(1, 8.5)
    mutator.propagate()
    println(output.toBuffer)
  }*/

  /*"ChunkSortTest" should "only reexecute the least possible" in {
    val mutator = new Mutator()
    val input = ChunkListInput[Int, Double](new ListConf(chunkSize = 2, partitions = 1))
    for (i <- List(10, 5, 6, 1, 7, 4, 8, 3, 2, 9)) {
      input.put(i, i)
    }
    val output = mutator.run(new ChunkSortTest(input))
    println(output.toBuffer)

    println("\npropagating")
    input.update(1, 8.5)
    mutator.propagate()
    println(output.toBuffer)
  }*/

  /*"SplitTest" should " asdf" in {
    val mutator = new Mutator()
    val input = ListInput[Int, Int](new ListConf(chunkSize = 1, partitions = 1))
    input.put(5, 5)
    input.put(3, 3)
    input.put(6, 6)
    input.put(7, 7)
    input.put(9, 9)
    input.put(2, 2)
    input.put(1, 1)
    input.put(4, 4)
    input.put(10, 10)

    val input2 = TableInput[Int, Int]()
    input2.put(1, 4)
    val output = mutator.run(new SplitTest(input, input2))
    println(output.read()._1 + "\n" + output.read()._2)

    println("\npropagating")
    //input2.update(1, 5)
    input.update(5, -1)
    mutator.propagate()
    println(output.read()._1 + "\n" + output.read()._2)
  }*/

  /*"MergeTest" should "asdf" in {
    val mutator = new Mutator()
    val conf = new ListConf(partitions = 1, chunkSize = 1)

    val input = ListInput[Int, Int](conf)
    /*input.put(1, 1)
    input.put(5, 5)
    input.put(6, 6)
    input.put(7, 7)
    input.put(10, 10)*/
    input.put(1, 1)
    input.put(6, 6)

    val input2 = ListInput[Int, Int](conf)
    /*input2.put(2, 2)
    input2.put(3, 3)
    input2.put(4, 4)
    input2.put(8, 8)
    input2.put(9, 9)*/
    input2.put(7, 7)

    //println(input.getAdjustableList())
    //println(input2.getAdjustableList())
    val output = mutator.run(new MergeTest(input, input2))
    println(output)
    println(output.toBuffer)

    println("\npropagating")
    input.remove(1)
    input.put(1, 8)

    //println(input.getAdjustableList())
    //println(input2.getAdjustableList())
    mutator.propagate()
    println(output)
    println(output.toBuffer)
  }*/

  /*"ChunkMergeTest" should "asdf" in {
    val mutator = new Mutator()
    val conf = new ListConf(partitions = 1, chunkSize = 2)

    val input = ChunkListInput[Int, Int](conf)

    val input2 = ChunkListInput[Int, Int](conf)
    //input2.put(0, 0)
    //input2.put(2, 2)
    input.put(4, 4)
    input.put(8, 8)

    val input2 = mutator.createChunkList[Int, Int](conf)
    input2.put(1, 1)
    input2.put(3, 3)
    input2.put(5, 5)
    input2.put(10, 10)

    println(input.getChunkList())
    println(input2.getChunkList())
    val output = mutator.run(new MergeTest(input, input2))
    println(output)
    println(output.toBuffer)

    println("\npropagating")
    input2.remove(3)
    input2.put(3, 12)

    println(input.getChunkList())
    println(input2.getChunkList())
    mutator.propagate()
    println(output)
    println(output.toBuffer)
  }*/

  /*"MapReduceTest" should "only reexecute the necessary parts" in {
    val mutator = new Mutator()
    val input = ListInput[Int, Int](new ListConf(partitions = 1, chunkSize = 1))
    for (i <- List(6, 2, 7, 3, 8, 1, 5, 10)) {
      input.put(i, i)
    }

    val output = mutator.run(new MapReduceTest(input))
    println(output.read())

    println("propagate")
    input.update(7, 8)
    mutator.propagate()
  }*/
}
