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

import tbd.Constants.Pointer
import tbd.ddg.{Ordering, Sublist, Timestamp}

class OrderingTests extends FlatSpec with Matchers {
  def checkOrdering(timestamps: List[Pointer]) {
    var previousPtr: Pointer = -1
    for (timestamp <- timestamps) {
      if (previousPtr != -1) {
        assert(previousPtr != -1)

        assert(Timestamp.<(previousPtr, timestamp))
        assert(Timestamp.>(timestamp, previousPtr))
      }

      previousPtr = timestamp
    }
  }

  def fill(ordering: Ordering, num: Int):
      Tuple3[Pointer, Pointer, Pointer] = {
    var i = 1
    val middle = ordering.after(-1, -1)
    while (i < num / 2) {
      ordering.after(-1, -1)
      i += 1
    }

    val start = ordering.after(-1, -1)
    val end = ordering.after(middle, -1)
    i += 2

    while (i < num) {
      ordering.after(middle, -1)
      i += 1
    }

    (start, middle, end)
  }

  val fillNums = 4 to 200
  "Ordering" should "return correctly ordered timestamps from after()" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      checkOrdering(List(tuple._1, tuple._2, tuple._3))
    }
  }

  it should "insert between timestamps correctly" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(tuple._1, -1)
      val t2 = ordering.after(tuple._2, -1)

      checkOrdering(List(tuple._1, t1, tuple._2, t2, tuple._3))
    }
  }

  it should "insert at the end correctly" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(tuple._3, -1)

      checkOrdering(List(tuple._1, tuple._2, tuple._3, t1))
    }
  }

  it should "insert at the beginning correctly" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(-1, -1)

      checkOrdering(List(t1, tuple._1, tuple._2, tuple._3))
    }
  }

  it should "insert correctly after removing the head" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      ordering.remove(tuple._1)
      val t1 = ordering.after(tuple._2, -1)
      val t2 = ordering.after(tuple._3, -1)
      val t3 = ordering.after(-1, -1)

      checkOrdering(List(t3, tuple._2, t1, tuple._3, t2))
    }
  }

  it should "insert correctly after removing the tail" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(tuple._3, -1)
      ordering.remove(t1)
      val t2 = ordering.after(tuple._1, -1)
      val t3 = ordering.after(tuple._2, -1)
      val t4 = ordering.after(tuple._3, -1)
      val t5 = ordering.after(-1, -1)

      checkOrdering(List(t5, tuple._1, t2, tuple._2, t3, tuple._3, t4))
    }
  }

  it should "insert correctly after removing from the middle" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(tuple._2, -1)
      ordering.remove(tuple._2)
      val t2 = ordering.after(-1, -1)
      val t3 = ordering.after(tuple._1, -1)
      val t4 = ordering.after(t1, -1)
      val t5 = ordering.after(tuple._3, -1)

      checkOrdering(List(t2, tuple._1, t3, t1, t4, tuple._3, t5))
    }
  }

  it should "append correctly" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val (start, middle, end) = fill(ordering, i)

      val t1 = ordering.append(-1)
      val t2 = ordering.after(end, -1)
      val t3 = ordering.after(t1, -1)

      checkOrdering(List(start, middle, end, t2, t1, t3))

      var appended: Pointer = -1
      var lastAppended: Pointer = -1
      for (j <- 1 to 5) {
        appended = ordering.append(-1)

        if (lastAppended != -1) {
          checkOrdering(List(start, middle, end, lastAppended, appended))
        } else {
          checkOrdering(List(start, middle, end, appended))
        }

        lastAppended = appended
      }
    }
  }
}
