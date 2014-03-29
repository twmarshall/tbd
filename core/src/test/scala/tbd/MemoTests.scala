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

import tbd.{Adjustable, Dest, Mutator, TBD}
import tbd.ddg.{MemoNode, ReadNode, RootNode}
import tbd.mod.Mod

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

class MemoTests extends FlatSpec with Matchers {
  "MemoTest" should "do stuff" in {
    val mutator = new Mutator()
    mutator.put(1, 1)
    mutator.put(2, 10)
    val test = new MemoTest()
    val output = mutator.run[Mod[Int]](test)
    output.read() should be (12)
    test.count should be (1)

    val root = new MockRootNode(List(
      new MockReadNode(List(
        new MockMemoNode(List(
          new MockReadNode(List())
        )),
        new MockReadNode(List())
      ))
    ))

    val ddg = mutator.getDDG()
    assert(root.isEqual(ddg.root))

    // Change the mod not read by the memoized function,
    // check that it isn't called.
    mutator.update(1, 3)
    mutator.propagate()
    output.read() should be (14)
    test.count should be (1)

    val root2 = new MockRootNode(List(
      new MockReadNode(List(
        new MockReadNode(List()),
        new MockMemoNode(List(
          new MockReadNode(List())
        )),
        new MockReadNode(List())
      ))
    ))

    val ddg2 = mutator.getDDG()
    assert(root2.isEqual(ddg2.root))

    // Change the other mod, the memoized function should
    // be called.
    mutator.update(1, 2)
    mutator.update(2, 8)
    mutator.propagate()
    output.read() should be (11)
    test.count should be (2)

    val ddg3 = mutator.getDDG()
    assert(root.isEqual(ddg3.root))

    mutator.shutdown()
  }
}
