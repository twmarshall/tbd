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

import tbd.util.OffHeapMap

class UtilTests extends FlatSpec with Matchers {
  "OffHeapMap" should "store an element" in {
    val mapPtr = OffHeapMap.create()

    OffHeapMap.increment(mapPtr, "asdf", 3)
    OffHeapMap.get(mapPtr, "asdf") should be (3)

    OffHeapMap.increment(mapPtr, "fdsa", 4)
    OffHeapMap.get(mapPtr, "fdsa") should be (4)
  }

  it should "update an existing element" in {
    val mapPtr = OffHeapMap.create()

    OffHeapMap.increment(mapPtr, "asdf", 1)
    OffHeapMap.get(mapPtr, "asdf") should be (1)

    OffHeapMap.increment(mapPtr, "asdf", 5)
    OffHeapMap.get(mapPtr, "asdf") should be (6)
  }

  it should "handle random insertions" in {
    val mapPtr = OffHeapMap.create()
    val controlMap = scala.collection.mutable.Map[String, Int]()
    val rand = new scala.util.Random()

    for (i <- 0 to 100) {
      val key = rand.nextString(3)
      val value = rand.nextInt(100)

      OffHeapMap.increment(mapPtr, key, value)

      if (controlMap.contains(key)) {
        controlMap(key) += value
      } else {
        controlMap(key) = value
      }
    }

    for ((key, value) <- controlMap) {
      OffHeapMap.get(mapPtr, key) should be (value)
    }
  }

  it should "handle random updates" in {
    val mapPtr = OffHeapMap.create()
    val controlMap = scala.collection.mutable.Map[String, Int]()
    val rand = new scala.util.Random()

    for (i <- 0 to 100) {
      val key = rand.nextString(3)
      val value = rand.nextInt(100)

      OffHeapMap.increment(mapPtr, key, value)

      if (controlMap.contains(key)) {
        controlMap(key) += value
      } else {
        controlMap(key) = value
      }
    }

    for (i <- 0 to 10) {
      var key = controlMap.head._1
      for ((_key, _value) <- controlMap) {
        if (rand.nextInt(10) == 1) {
          key = _key
        }
      }

      val value = rand.nextInt(100)
      OffHeapMap.increment(mapPtr, key, value)

      controlMap(key) += value
    }

    for ((key, value) <- controlMap) {
      OffHeapMap.get(mapPtr, key) should be (value)
    }
  }

  it should "merge correctly" in {
    val map1Ptr = OffHeapMap.create()
    OffHeapMap.increment(map1Ptr, "asdf", 3)
    OffHeapMap.increment(map1Ptr, "fdsa", 4)

    val map2Ptr = OffHeapMap.create()
    OffHeapMap.increment(map2Ptr, "asdf", 2)
    OffHeapMap.increment(map2Ptr, "qwer", 6)

    val merged = OffHeapMap.merge(map1Ptr, map2Ptr)

    OffHeapMap.get(merged, "asdf") should be (5)
    OffHeapMap.get(merged, "fdsa") should be (4)
    OffHeapMap.get(merged, "qwer") should be (6)
  }
}
