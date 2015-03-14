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
package tdb.test

import org.scalatest._
import scala.collection.mutable.ArrayBuffer

import tdb._
import tdb.list._
import tdb.TDB._

class ChangePropagationTests extends FlatSpec with Matchers {

  class PropagationOrderTest(one: Mod[Int])
    extends Adjustable[Mod[Int]] {
    var num = 0

    def run(implicit c: Context) = {
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

  "PropagationOrderTest" should "reexecute reads in the correct order" in {
    val mutator = new Mutator()
    val one = mutator.createMod(1)
    val test = new PropagationOrderTest(one)
    val output = mutator.run(test)
    test.num should be (2)

    test.num = 0
    mutator.updateMod(one, 2)
    mutator.propagate()
    test.num should be (2)

    mutator.shutdown()
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

  "PropagationOrderTest2" should "reexecute map in the correct order" in {
    val mutator = new Mutator()
    val input = mutator.createList[Int, Int](ListConf(partitions = 1))

    for (i <- 0 to 100) {
      input.put(i, i)
    }
    val test = new PropagationOrderTest2(input)
    mutator.run(test)

    for (i <- 0 to 100) {
      input.put(i, i + 1)
    }

    mutator.propagate()

    mutator.shutdown()
  }

  class ParTest(one: Mod[Int]) extends Adjustable[Mod[Int]] {
    def run(implicit c: Context) = {

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

  "ParTest" should "do something" in {
    val mutator = new Mutator()
    val one = mutator.createMod(1)
    val output = mutator.run(new ParTest(one))
    mutator.read(output) should be (4)

    mutator.updateMod(one, 2)
    mutator.propagate()

    mutator.read(output) should be (6)

    mutator.shutdown()
  }


  // Checks that modNoDest returns the correct values even after a convuloted
  // series of memo matches.
  class ModNoDestTest
      (one: Mod[Int],
       two: Mod[Int],
       three: Mod[Int],
       four: Mod[Int],
       five: Mod[Int],
       six: Mod[Int],
       seven: Mod[Int])
    extends Adjustable[Mod[Int]] {
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
    val memo = new Memoizer[Changeable[Int]]()

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

  "ModNoDestTest" should "update the dests for the memo matches" in {
    val mutator = new Mutator()
    val one = mutator.createMod(1)
    val two = mutator.createMod(2)
    val three = mutator.createMod(3)
    val four = mutator.createMod(4)
    val five = mutator.createMod(5)
    val six = mutator.createMod(6)
    val seven = mutator.createMod(7)
    val output = mutator.run(new ModNoDestTest(
      one, two, three, four, five, six, seven))
    mutator.read(output) should be (6)

    mutator.updateMod(one, 2)
    mutator.updateMod(four, 5)
    mutator.propagate()
    mutator.read(output) should be (7)

    mutator.updateMod(one, 1)
    mutator.updateMod(seven, 8)
    mutator.propagate()
    mutator.read(output) should be (8)

    mutator.shutdown()
  }
}
