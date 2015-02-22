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
package tdb.util

import java.io.Serializable
import scala.collection.mutable.{Buffer, Map}

object ObjHasher {
  def makeHasher[T](_range: HashRange, objs: Buffer[T]) = {
    val range =
      if (_range.rangeSize() != objs.size && _range.isComplete()) {
        new HashRange(0, objs.size, objs.size)
      } else {
        _range
      }

    assert(
      range.rangeSize() == objs.size,
      "ObjHasher.makeHasher rangeSize " + range.rangeSize() + " != " +
      objs.size)

    val map = Map[Int, T]()
    for (i <- range.range()) {
      map(i) = objs(i - range.min)
    }
    new ObjHasher(map, range.total)
  }

  def makeHasher[T](range: HashRange, obj: T) = {
    val objs = Map[Int, T]()
    for (i <- range.range()) {
      objs(i) = obj
    }
    new ObjHasher(objs, range.total)
  }

  def combineHashers[T](hash1: ObjHasher[T], hash2: ObjHasher[T]) = {
    assert(
      hash1.total == hash2.total,
      "ObjHasher.combineHashers - hash range totals " +
      hash1.total + " != " + hash2.total)

    val objs = hash1.objs ++ hash2.objs

    assert(
      objs.size == hash1.objs.size + hash2.objs.size,
      "ObjHasher.combineHashers combined size " + objs.size + " != " +
      hash1.objs.size + " + " + hash2.objs.size)

    new ObjHasher(objs, hash1.total)
  }
}

class ObjHasher[T](val objs: Map[Int, T], val total: Int)
  extends Serializable {
  def hash(a: Any): Int = a.hashCode().abs % total

  def getObj(a: Any): T = {
    val hashValue = hash(a)
    objs(hashValue)
  }

  def isComplete(): Boolean = {
    var complete = true

    for (i <- 0 until total) {
      if (!objs.contains(i)) {
        complete = false
      }
    }

    complete
  }

  def hashAll[T, U](values: Iterable[(T, U)]) = {
    val hashed = Map[Int, Buffer[(T, U)]]()
    for (i <- 0 until total) {
      hashed(i) = Buffer[(T, U)]()
    }

    for (pair <- values) {
      hashed(hash(pair._1)) += pair
    }

    hashed
  }
}
