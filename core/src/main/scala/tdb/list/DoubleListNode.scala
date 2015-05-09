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
package tdb.list

import java.io.Serializable
import scala.collection.mutable.Map

import tdb._
import tdb.TDB._

object DoubleListNode {
  type ChangeableTuple[T, U] =
    (Changeable[DoubleListNode[T, U]], Changeable[DoubleListNode[T, U]])
}

class DoubleListNode[T, U]
    (var valueMod: Mod[(T, U)],
     val nextMod: Mod[DoubleListNode[T, U]]) extends Serializable {

  def foreach[V, W]
      (f: ((T, U), Context) => Unit,
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    //readAny(valueMod) {
      //case value => f(value, c)
    //}
    val value = c.read(valueMod)
    f(value, c)

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.foreach(f, memo)
        }
    }
  }

  def map[V, W]
      (f: ((T, U)) => (V, W),
       memo: Memoizer[Mod[DoubleListNode[V, W]]])
      (implicit c: Context): Changeable[DoubleListNode[V, W]] = {
    val newValue = mod {
      read(valueMod) {
        case v => write(f(v))
      }
      //write(null.asInstanceOf[(V, W)])
    }

    val newNextMod = memo(nextMod) {
      mod {
        read(nextMod) {
          case null =>
            write[DoubleListNode[V, W]](null)
          case next =>
            next.map(f, memo)
        }
      }
    }

    write(new DoubleListNode[V, W](newValue, newNextMod))
  }


  def mapValues[V]
      (f: U => V,
       memo: Memoizer[Changeable[DoubleListNode[T, V]]])
      (implicit c: Context): Changeable[DoubleListNode[T, V]] = {
    val newValueMod = mod {
      read(valueMod) {
        case (k, v) =>
          write((k, f(v)))
      }
    }

    val newNextMod = mod {
      read(nextMod) {
        case null =>
          write[DoubleListNode[T, V]](null)
        case next =>
          memo(next) {
            next.mapValues(f, memo)
          }
      }
    }

    write(new DoubleListNode(newValueMod, newNextMod))
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[DoubleListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[DoubleListNode[T, U]]
      that.valueMod == valueMod && that.nextMod == nextMod
    }
  }

  override def hashCode() = valueMod.hashCode() * nextMod.hashCode()

  override def toString = "Node(" + valueMod + ", " + nextMod + ")"
}
