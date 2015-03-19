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

  def chunkMap[V, W]
      (f: Iterable[(T, U)] => (V, W),
       memo: Memoizer[Mod[DoubleListNode[V, W]]])
      (implicit c: Context): Changeable[DoubleListNode[V, W]] = {
    val newChunkMod = mod {
      read(chunkMod) {
        case chunk => write(f(chunk))
      }
    }

    val newNextMod = memo(nextMod) {
      mod {
        read(nextMod) {
          case null =>
            write[DoubleListNode[V, W]](null)
          case next =>
            next.chunkMap(f, memo)
        }
      }
    }

    write(new DoubleListNode[V, W](newChunkMod, newNextMod))
  }

  def flatMap[V, W]
      (f: ((T, U)) => Iterable[(V, W)],
       memo: Memoizer[Mod[DoubleChunkListNode[V, W]]])
      (implicit c: Context): Changeable[DoubleChunkListNode[V, W]] = {
    val newChunkMod = mod {
      read(chunkMod) {
        // TODO: we should try to maintain chunk sizes here.
        case chunk => write(chunk.flatMap(f))
      }
    }

    val newNextMod = memo(nextMod) {
      mod {
        read(nextMod) {
          case null =>
            write[DoubleChunkListNode[V, W]](null)
          case next =>
            next.flatMap(f, memo)
        }
      }
    }

    write(new DoubleChunkListNode[V, W](newChunkMod, newNextMod))
  }

  def foreach[V, W]
      (f: ((T, U), Context) => Unit,
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    readAny(chunkMod) {
      case chunk => for (value <- chunk) f(value, c)
    }

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.foreach(f, memo)
        }
    }
  }

  def foreachChunk[V, W]
      (f: (Iterable[(T, U)], Context) => Unit,
       memo: Memoizer[Unit])
      (implicit c: Context): Unit = {
    readAny(chunkMod) {
      case chunk => f(chunk, c)
    }

    readAny(nextMod) {
      case null =>
      case node =>
        memo(node) {
          node.foreachChunk(f, memo)
        }
    }
  }

  def map[V, W]
      (f: ((T, U)) => (V, W),
       memo: Memoizer[Mod[DoubleChunkListNode[V, W]]])
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
            next.map(f, memo)
        }
      }
    }

    write(new DoubleChunkListNode[V, W](newChunkMod, newNextMod))
  }

  def mapValues[V]
      (f: U => V,
       memo: Memoizer[Changeable[DoubleChunkListNode[T, V]]])
      (implicit c: Context): Changeable[DoubleChunkListNode[T, V]] = {
    val newChunkMod = mod {
      read(chunkMod) {
        case chunk =>
          write(for ((k, v) <- chunk) yield (k, f(v)))
      }
    }

    val newNextMod = mod {
      read(nextMod) {
        case null =>
          write[DoubleChunkListNode[T, V]](null)
        case next =>
          memo(next) {
            next.mapValues(f, memo)
          }
      }
    }

    write(new DoubleChunkListNode(newChunkMod, newNextMod))
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
