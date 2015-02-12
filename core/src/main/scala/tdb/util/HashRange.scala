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

/**
 * Represents a hash partitioning range [min, max)
 */
class HashRange(val min: Int, val max: Int, val total: Int)
  extends Serializable {

  def hash(a: Any): Int = a.hashCode().abs % total

  def fallsInside(a: Any): Boolean = {
    val hashValue = hash(a)
    hashValue >= min && hashValue < max
  }

  def rangeSize() = max - min

  def range() = min until max

  def isComplete() = min == 0 && max == total

  override def equals(a: Any): Boolean = {
    a match {
      case h: HashRange =>
        if (isComplete() && h.isComplete())
          true
        else
          min == h.min && max == h.max && total == h.total
      case _ => false
    }
  }

  override def toString() = "Range(" + min + ", " + max + ", " + total + ")"
}
