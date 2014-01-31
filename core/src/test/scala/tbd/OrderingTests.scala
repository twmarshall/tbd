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

import tbd.ddg.{Ordering, Timestamp}

class OrderingTests extends FlatSpec with Matchers {
  def checkOrdering(timestamps: List[Timestamp]) {
    var previous: Timestamp = null
    for (timestamp <- timestamps) {
      if (previous != null) {
	assert(previous < timestamp)
	assert(timestamp > previous)
      }
      previous = timestamp
    }
  }

  "Ordering" should "return correctly ordered timestamps from after()" in {
    val ordering = new Ordering()

    val t1 = ordering.after(null)
    val t2 = ordering.after(t1)

    checkOrdering(List(t1, t2))
  }


  it should "insert between timestamps correctly" in {
    val ordering = new Ordering()

    val t1 = ordering.after(null)
    val t2 = ordering.after(t1)
    val t3 = ordering.after(t1)

    checkOrdering(List(t1, t3, t2))
  }

  it should "insert at the end correctly" in {
    val ordering = new Ordering()

    val t1 = ordering.after(null)
    val t2 = ordering.after(t1)
    val t3 = ordering.after(t1)
    val t4 = ordering.after(t2)

    checkOrdering(List(t1, t3, t2, t4))
  }

  it should "insert at the beginning correctly" in {
    val ordering = new Ordering()

    val t1 = ordering.after(null)
    val t2 = ordering.after(t1)
    val t3 = ordering.after(t1)
    val t4 = ordering.after(t2)
    val t5 = ordering.after(null)

    checkOrdering(List(t5, t1, t3, t2, t4))
  }

  it should "insert correctly after removing the head" in {
    val ordering = new Ordering()

    val t1 = ordering.after(null)
    val t2 = ordering.after(t1)
    val t3 = ordering.after(t1)
    val t4 = ordering.after(t2)
    val t5 = ordering.after(null)
    ordering.remove(t5)
    val t6 = ordering.after(t3)
    val t7 = ordering.after(t4)
    val t8 = ordering.after(null)

    checkOrdering(List(t8, t1, t3, t6, t2, t4, t7))
  }

  it should "insert correctly after removing the tail" in {
    val ordering = new Ordering()

    val t1 = ordering.after(null)
    val t2 = ordering.after(t1)
    val t3 = ordering.after(t1)
    val t4 = ordering.after(t2)
    val t5 = ordering.after(null)
    ordering.remove(t5)
    val t6 = ordering.after(t3)
    val t7 = ordering.after(t4)
    val t8 = ordering.after(null)
    ordering.remove(t7)
    val t9 = ordering.after(null)
    val t10 = ordering.after(t4)
    val t11 = ordering.after(t3)

    checkOrdering(List(t9, t8, t1, t3, t11, t6, t2, t4, t10))
  }

  it should "insert correctly after removing from the middle" in {
    val ordering = new Ordering()

    val t1 = ordering.after(null)
    val t2 = ordering.after(t1)
    val t3 = ordering.after(t1)
    val t4 = ordering.after(t2)
    val t5 = ordering.after(null)
    ordering.remove(t5)
    val t6 = ordering.after(t3)
    val t7 = ordering.after(t4)
    val t8 = ordering.after(null)
    ordering.remove(t7)
    val t9 = ordering.after(null)
    val t10 = ordering.after(t4)
    val t11 = ordering.after(t3)
    ordering.remove(t11)
    val t12 = ordering.after(t3)
    val t13 = ordering.after(t6)
    val t14 = ordering.after(null)
    val t15 = ordering.after(t10)

    checkOrdering(List(t14, t9, t8, t1, t3, t12, t6, t13, t2, t4, t10, t15))
  }
}
