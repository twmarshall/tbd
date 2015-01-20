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

class DoubleChunkListNode[T, U]
    (var chunkMod: Mod[Vector[(T, U)]],
     val nextMod: Mod[DoubleChunkListNode[T, U]],
     val size: Int = 0) extends Serializable {

  def hashChunkMap[V, W]
      (f: Iterable[(T, U)] => Iterable[(V, W)],
       input: ListInput[V, W],
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    readAny(chunkMod) {
      case chunk =>
        val out = f(chunk)
        for (pair <- out) {
          put(input, pair._1, pair._2)
        }
    }

    readAny(nextMod) {
      case null => write[DoubleListNode[V, W]](null)
      case node =>
        memo(node) {
          node.hashChunkMap(f, input, memo)
        }
    }
  }

  def hashPartitionedFlatMap[V, W]
      (f: ((T, U)) => Iterable[(V, W)],
       input: ListInput[V, W],
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    readAny(chunkMod) {
      case chunk =>
        for (v <- chunk) {
          val out = f(v)
          for (pair <- out) {
            put(input, pair._1, pair._2)
          }
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
       memo: Memoizer[Mod[DoubleChunkListNode[V, W]]],
       modizer: Modizer1[DoubleChunkListNode[V, W]])
      (implicit c: Context): Changeable[DoubleChunkListNode[V, W]] = {
    val newChunkMod = mod {
      read(chunkMod) {
        case chunk => write(for (v <- chunk) yield f(v))
      }
    }

    val newNextMod = memo(nextMod) {
      mod {
        read(nextMod) {
          case null =>
            write[DoubleChunkListNode[V, W]](null)
          case next =>
            next.map(f, memo, modizer)
        }
      }
    }

    write(new DoubleChunkListNode[V, W](newChunkMod, newNextMod))
  }

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[DoubleChunkListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[DoubleChunkListNode[T, U]]
      that.chunkMod == chunkMod && that.nextMod == nextMod
    }
  }

  override def hashCode() = chunkMod.hashCode() * nextMod.hashCode()

  override def toString = "Node(" + chunkMod + ", " + nextMod + ")"
}
