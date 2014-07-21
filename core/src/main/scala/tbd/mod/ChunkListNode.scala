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
package tbd.mod

import java.io.Serializable

import tbd.{Changeable, Memoizer, TBD}
import tbd.TBD._

// The default value of zero for size works because size is only ever
// accessed by the Modifier, which will set it appropriately.
class ChunkListNode[T, U](
    val chunk: Vector[(T, U)],
    val nextMod: Mod[ChunkListNode[T, U]],
    val size: Int = 0) extends Serializable {

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ChunkListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[ChunkListNode[T, U]]
      that.chunk == chunk && that.nextMod == nextMod
    }
  }

  def map[V, W](
      f: (TBD, (T, U)) => (V, W),
      memo: Memoizer[Changeable[ChunkListNode[V, W]]])
     (implicit tbd: TBD): Changeable[ChunkListNode[V, W]] = {
    val newChunk = chunk.map((pair: (T, U)) => f(tbd, pair))
    val newNext = mod {
      read(nextMod) {
	case null => write[ChunkListNode[V, W]](null)
	case next =>
          memo(nextMod) {
            next.map(f, memo)
          }
      }
    }

    write(new ChunkListNode[V, W](newChunk, newNext))
  }

  def chunkMap[V, Q](
      tbd: TBD,
      f: (TBD, Vector[(T, U)]) => (V, Q),
      memo: Memoizer[Mod[ModListNode[V, Q]]])
        : Changeable[ModListNode[V, Q]] = {
    val newNextMod = memo(nextMod) {
      tbd.mod {
        tbd.read(nextMod)(next => {
          if (next != null && next.chunk.size > 0)
            next.chunkMap(tbd, f, memo)
          else
            tbd.write[ModListNode[V, Q]](null)
        })
      }
    }

    tbd.write(new ModListNode[V, Q](f(tbd, chunk), newNextMod))
  }

  def print = "ChunkListNode(" + chunk + ")"
}
