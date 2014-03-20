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
import tbd.mod.{Matrix, Mod}

class ArrayMapTest extends Adjustable {
  def run(tbd: TBD): Array[Mod[String]] = {
    val array = tbd.input.getArray[Mod[String]]()
    tbd.map(array, (_: String) + " mapped")
  }
}

class MatrixMultTest extends Adjustable {
  def run(tbd: TBD): Matrix = {
    val one = tbd.input.get[Matrix](1)
    val two = tbd.input.get[Matrix](2)

    one.mult(tbd, two)
  }
}

class MemoTest extends Adjustable {
  // Note: real client applications should NOT have mutable state like this.
  // We are just using it to ensure that the memoized function doesn't get
  // reexecuted as appropriate.
  var count = 0

  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.get[Mod[Int]](1)
    val two = tbd.input.get[Mod[Int]](2)
    val memo = tbd.memo[Int, Int]()

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one, (oneValue: Int) => {
        if (oneValue == 3) {
          tbd.mod((dest: Dest[Int]) => {
            tbd.read(one, (oneValueAgain: Int) => {
              tbd.write(dest, oneValueAgain)
            })
          })
        }
        val memoMod = tbd.mod((memoDest: Dest[Int]) => {
          memo(List(two))(() => {
	    count += 1
	    tbd.read(two, (valueTwo: Int) => {
	      tbd.write(memoDest, valueTwo + 1)
	    })
          })
        })

        tbd.read(memoMod, (memoValue: Int) => {
          tbd.write(dest, oneValue + memoValue)
        })
      })
    })
  }
}

class PropagationOrderTest extends Adjustable {
  var num = 0

  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.get[Mod[Int]](1)

    tbd.mod((dest: Dest[Int]) => {
      tbd.read(one, (v1: Int) => {
        assert(num == 0)
        num += 1
        tbd.read(one, (v2: Int) => {
          assert(num == 1)
          num += 1
          tbd.write(dest, v2)
        })
      })

      tbd.read(one, (v3: Int) => {
        assert(num == 2)
        tbd.write(dest, v3)
      })
    })
  }
}

class TestSpec extends FlatSpec with Matchers {
  "ArrayMapTest" should "return a correctly mapped array" in {
    val mutator = new Mutator()
    mutator.put(1, "one")
    mutator.put(2, "two")
    val output = mutator.run[Array[Mod[String]]](new ArrayMapTest())
    output.deep.mkString(", ") should be ("two mapped, one mapped")

    mutator.update(1, "three")
    mutator.propagate[Array[Mod[String]]]()
    output.deep.mkString(", ") should be ("two mapped, three mapped")

    mutator.shutdown()
  }

  /*"MatrixMult" should "do stuff" in {
    val mutator = new Mutator()
    mutator.putMatrix(1, Array(Array(1, 3)))
    mutator.putMatrix(2, Array(Array(5), Array(6)))
    val output = mutator.run(new MatrixMultTest())
    mutator.shutdown()
  }*/

  "MemoTest" should "do stuff" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    mutator.put(2, 10)
    val test = new MemoTest()
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (12)
    test.count should be (1)

    // Change the mod not read by the memoized function,
    // check that it isn't called.
    mutator.update(1, 3)
    mutator.propagate()
    output.read() should be (14)
    test.count should be (1)

    // Change the other mod, the memoized function should
    // be called.
    mutator.update(1, 2)
    mutator.update(2, 8)
    mutator.propagate()
    output.read() should be (11)
    test.count should be (2)

    mutator.shutdown()
  }

  "PropagationOrderTest" should "reexecute reads in the correct order" in {
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
}
