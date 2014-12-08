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
package tbd.list

import java.io.Serializable
import scala.collection.mutable.Map

import tbd._
import tbd.TBD._

class DoubleListNode[T, U]
    (var value: Mod[(T, U)],
     val nextMod: Mod[DoubleListNode[T, U]]) extends Serializable {

  def hashPartitionedFlatMap[V, W]
      (f: ((T, U)) => Iterable[(V, W)],
       input: ListInput[V, W],
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    readAny(value) {
      case v =>
        val out = f(v)
        for (pair <- out) {
          put(input, pair._1, pair._2)
        }
    }

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.hashPartitionedFlatMap(f, input, memo)
        }
    }
  }

  def map[V, W]
      (f: ((T, U)) => (V, W),
       memo: Memoizer[Mod[DoubleListNode[V, W]]],
       modizer: Modizer1[DoubleListNode[V, W]])
      (implicit c: Context): Changeable[DoubleListNode[V, W]] = {
    val newValue = mod {
      read(value) {
        case v => write(f(v))
      }
    }

    val newNextMod = memo(nextMod) {
      mod {
        read(nextMod) {
          case null =>
            write[DoubleListNode[V, W]](null)
          case next =>
            next.map(f, memo, modizer)
        }
      }
    }

    write(new DoubleListNode[V, W](newValue, newNextMod))
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[DoubleListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[DoubleListNode[T, U]]
      that.value == value && that.nextMod == nextMod
    }
  }

  override def hashCode() = value.hashCode() * nextMod.hashCode()

  override def toString = "Node(" + value + ", " + nextMod + ")"
}
