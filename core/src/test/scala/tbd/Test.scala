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
  def run(dest: Dest, tbd: TBD): Changeable[Any] = {
    val array = tbd.input.getArray[Mod[String]]()
    println(array.getClass)
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
    val one = tbd.input.get[Matrix](1)
    val two = tbd.input.get[Matrix](2)

    tbd.write(dest, one.mult(tbd, two)).asInstanceOf[Changeable[Any]]
  }
}

class MemoTest extends Adjustable {
  def run(dest: Dest, tbd: TBD): Changeable[Any] = {
    val one = tbd.input.get[Mod[Int]](1)
    val two = tbd.input.get[Mod[Int]](2)
    val memo = tbd.memo[Int, Int]()

    tbd.read(one, (valueOne: Int) => {
      if (valueOne == 1) {
	      memo(List(two))(() => {
	        println("memoized func")
	        tbd.read(two, valueTwo => tbd.write(dest, valueTwo + 1))
	      })
      } else {
	      memo(List(two)) (() => {
	        println("memoized func 2")
	        tbd.read(two, valueTwo => tbd.write(dest, valueTwo + 2))
	      })
      }
    }).asInstanceOf[Changeable[Any]]
  }
}

class TestSpec extends FlatSpec with Matchers {
  "ArrayMapTest" should "return a correctly mapped array" in {
    val test = new Mutator()
    val oneMod = test.input.putMod(1, "one")
    test.input.putMod(2, "two")
    val output = test.run[Array[Mod[String]]](new ArrayMapTest())
    output.read().deep.mkString(", ") should be ("two mapped, one mapped")

    oneMod.update("three")
    test.input.putMod(4, "four")
    val propOutput = test.propagate[Array[Mod[String]]]()
    propOutput.read().deep.mkString(", ") should be ("two mapped, four mapped, three mapped")

    test.shutdown()
  }

  "ListMapTest" should "return a correctly mapped list" in {
    val test = new Mutator()
    test.input.putMod(1, "one")
    test.input.putMod(2, "two")
    val output = test.run(new ListMapTest())
    output.read().toString should be ("(one mapped, two mapped)")
    test.shutdown()
  }

  "MatrixMult" should "do stuff" in {
    val test = new Mutator()
    test.input.putMatrix(1, Array(Array(1, 3)))
    test.input.putMatrix(2, Array(Array(5), Array(6)))
    val output = test.run(new MatrixMultTest())
    test.shutdown()
  }

  "MemoTest" should "do stuff" in {
    val test = new Mutator()
    test.input.putMod(1, 1)
    test.input.putMod(2, 10)
    val output = test.run(new MemoTest())
    println(output.read())
    println(test.propagate().read())
    test.shutdown()
  }
}
