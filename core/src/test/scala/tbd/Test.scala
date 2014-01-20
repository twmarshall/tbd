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
import tbd.mod.Matrix

class ArrayMapTest extends Adjustable {
  def run(dest: Dest, tbd: TBD): Changeable[Any] = {
    val array = tbd.input.getArray()
    val mappedArray = tbd.map(array, (_: String) + " mapped")
    tbd.write(dest, mappedArray)
  }
}

class ListMapTest extends Adjustable {
  def run(dest: Dest, tbd: TBD): Changeable[Any] = {
    val list = tbd.input.getList()
    val mappedList = tbd.parMap(list, (_: String) + " mapped")
    tbd.write(dest, mappedList)
  }
}

class MatrixMultTest extends Adjustable {
  def run(dest: Dest, tbd: TBD): Changeable[Any] = {
    println("??")
    val one = tbd.input.get[Matrix](1)
    println(one)
    val two = tbd.input.get[Matrix](2)
    println(two)

    tbd.read(one, (mat1: Matrix) => tbd.read(two, (mat2: Matrix) => tbd.write(dest, mat1.mult(tbd, mat2)))).asInstanceOf[Changeable[Any]]
    //tbd.write(dest, one)
  }
}

class TestSpec extends FlatSpec with Matchers {
  "ArrayMapTest" should "return a correctly mapped array" in {
    val test = new Mutator()
    test.input.put(1, "one")
    test.input.put(2, "two")
    val output = test.run(new ArrayMapTest())
    output.get() should be (Array("two mapped", "one mapped"))
    test.shutdown()
  }

  "ListMapTest" should "return a correctly mapped list" in {
    val test = new Mutator()
    test.input.put(1, "one")
    test.input.put(2, "two")
    val output = test.run(new ListMapTest())
    output.get().toString should be ("(one mapped, two mapped)")
    test.shutdown()
  }

  "MatrixMult" should "do stuff" in {
    val test = new Mutator()
    test.input.putMatrix(1, Array(Array(1, 3)))
    test.input.putMatrix(2, Array(Array(5), Array(6)))
    val output = test.run(new MatrixMultTest())
    println(output.get())
    test.shutdown()
  }
}
