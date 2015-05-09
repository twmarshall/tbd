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
}
