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

import tbd.Changeable
import tbd.Dest
import tbd.Mutator
import tbd.TBD
import tbd.ListNode
import tbd.mod.Mod

class ArrayMapTest extends TBD {
  def run(dest: Dest): Changeable[Any] = {
    val array = input.getArray()
    val mappedArray = map(array, (_: String) + " mapped")
    write(dest, mappedArray)
  }
}

class ListMapTest extends TBD {
  def run(dest: Dest): Changeable[Any] = {
    val list = input.getList()
    val mappedList = parMap(list, (_: String) + " mapped")
    write(dest, mappedList)
  }
}

class TestSpec extends FlatSpec with Matchers {
  "ArrayMapTest" should "return a correctly mapped array" in {
    val test = new Mutator[ArrayMapTest]()
    test.input.put(1, "one")
    test.input.put(2, "two")
    val output = test.run()
    output.get() should be (Array("two mapped", "one mapped"))
    test.shutdown()
  }

  "ListMapTest" should "return a correctly mapped list" in {
    val test = new Mutator[ListMapTest]()
    test.input.put(1, "one")
    test.input.put(2, "two")
    val output = test.run()
    output.get().toString should be ("(one mapped, two mapped)")
    test.shutdown()
  }
}
