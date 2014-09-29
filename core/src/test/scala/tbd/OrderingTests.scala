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

import tbd.ddg.{Ordering, Sublist, Timestamp}

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

  def fill(ordering: Ordering, num: Int):
      Tuple3[Timestamp, Timestamp, Timestamp] = {
    var i = 1
    val middle = ordering.after(null)
    while (i < num / 2) {
      ordering.after(null)
      i += 1
    }

    val start = ordering.after(null)
    val end = ordering.after(middle)
    i += 2

    while (i < num) {
      ordering.after(middle)
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

      val t1 = ordering.after(tuple._1)
      val t2 = ordering.after(tuple._2)

      checkOrdering(List(tuple._1, t1, tuple._2, t2, tuple._3))
    }
  }

  it should "insert at the end correctly" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(tuple._3)

      checkOrdering(List(tuple._1, tuple._2, tuple._3, t1))
    }
  }

  it should "insert at the beginning correctly" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(null)

      checkOrdering(List(t1, tuple._1, tuple._2, tuple._3))
    }
  }

  it should "insert correctly after removing the head" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      ordering.remove(tuple._1)
      val t1 = ordering.after(tuple._2)
      val t2 = ordering.after(tuple._3)
      val t3 = ordering.after(null)

      checkOrdering(List(t3, tuple._2, t1, tuple._3, t2))
    }
  }

  it should "insert correctly after removing the tail" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(tuple._3)
      ordering.remove(t1)
      val t2 = ordering.after(tuple._1)
      val t3 = ordering.after(tuple._2)
      val t4 = ordering.after(tuple._3)
      val t5 = ordering.after(null)

      checkOrdering(List(t5, tuple._1, t2, tuple._2, t3, tuple._3, t4))
    }
  }

  it should "insert correctly after removing from the middle" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val tuple = fill(ordering, i)

      val t1 = ordering.after(tuple._2)
      ordering.remove(tuple._2)
      val t2 = ordering.after(null)
      val t3 = ordering.after(tuple._1)
      val t4 = ordering.after(t1)
      val t5 = ordering.after(tuple._3)

      checkOrdering(List(t2, tuple._1, t3, t1, t4, tuple._3, t5))
    }
  }

  it should "append correctly" in {
    for (i <- fillNums) {
      val ordering = new Ordering()
      val (start, middle, end) = fill(ordering, i)

      val t1 = ordering.append()
      val t2 = ordering.after(end)
      val t3 = ordering.after(t1)

      checkOrdering(List(start, middle, end, t2, t1, t3))

      var appended: Timestamp = null
      var lastAppended: Timestamp = null
      for (j <- 1 to 5) {
	appended = ordering.append()

	if (lastAppended != null) {
	  checkOrdering(List(start, middle, end, lastAppended, appended))
	} else {
	  checkOrdering(List(start, middle, end, appended))
	}

	lastAppended = appended
      }
    }
  }
}
