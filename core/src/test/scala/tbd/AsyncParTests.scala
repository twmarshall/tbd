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

class AsyncRaceConditionTest extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.getMod[Int](1)

    var isAsync = true

    val two = tbd.asyncMod((tbd: TBD, dest: Dest[Int]) => {
        tbd.read(one)(one => {
          Thread.sleep(1000)
          isAsync = false
          tbd.write(dest, one + 1)
        })
    })

    Thread.sleep(200)

    assert(isAsync)

    val three = tbd.mod((dest: Dest[Int]) => {
        tbd.read(two)(two => {
           tbd.write(dest, two + 1)
        })
    })

    three
  }
}

class ParRaceConditionTest extends Adjustable {
  def run(tbd: TBD): Mod[Int] = {
    val one = tbd.input.getMod[Int](1)

    val (two, three) = tbd.par((tbd: TBD) => {
      tbd.mod((dest: Dest[Int]) => {
        tbd.read(one)(one => {
          tbd.write(dest, one + 1)
        })
      })
    }, (tbd: TBD) => {
      tbd.mod((dest: Dest[Int]) => {
        tbd.write(dest, 3)
      })
    })

    val five = tbd.mod((dest: Dest[Int]) => {
      tbd.read(two)(two => {
        tbd.read(three)(three => {
          tbd.write(dest, two + three)
        })
      })
    })

    five
  }
}

class AsyncParTests extends FlatSpec with Matchers {
  "AsyncRaceConditionTest" should "prevent a race condition during " +
      "change propagation" in {
    val mutator = new Mutator()
    mutator.put(1, 1)

    val output = mutator.run[Mod[Int]](new AsyncRaceConditionTest())
    output.read() should be (3)

    mutator.update(1, 2)
    mutator.propagate()

    output.read() should be (4)

    mutator.shutdown()
  }

  "ParRaceConditionTest" should "prevent a race condition during " +
      "change propagation" in {
    val mutator = new Mutator()
    mutator.put(1, 1)

    val output = mutator.run[Mod[Int]](new ParRaceConditionTest())
    output.read() should be (5)

    mutator.update(1, 2)
    mutator.propagate()

    output.read() should be (6)

    mutator.shutdown()
  }
}
