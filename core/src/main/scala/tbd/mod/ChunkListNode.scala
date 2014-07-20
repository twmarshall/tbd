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
      tbd: TBD,
      f: (TBD, (T, U)) => (V, W),
      dest: Dest[ChunkListNode[V, W]],
      memo: Memoizer[Changeable[ChunkListNode[V, W]]])
        : Changeable[ChunkListNode[V, W]] = {
    val newChunk = chunk.map((pair: (T, U)) => f(tbd, pair))
    val newNext = tbd.mod((dest: Dest[ChunkListNode[V, W]]) => {
      tbd.read(nextMod)(next => {
        if (next != null) {
          memo(nextMod, dest) {
            next.map(tbd, f, dest, memo)
          }
        } else {
          tbd.write(dest, null)
        }
      })
    })

    tbd.write(dest, new ChunkListNode[V, W](newChunk, newNext))
  }

  def chunkMap[V, Q](
      tbd: TBD,
      dest: Dest[ModListNode[V, Q]],
      f: (TBD, Vector[(T, U)]) => (V, Q),
      memo: Memoizer[Mod[ModListNode[V, Q]]])
        : Changeable[ModListNode[V, Q]] = {
    val newNextMod = memo(nextMod) {
      tbd.mod((dest: Dest[ModListNode[V, Q]]) =>
        tbd.read(nextMod)(next => {
          if (next != null && next.chunk.size > 0)
            next.chunkMap(tbd, dest, f, memo)
          else
            tbd.write(dest, null)
        }))
    }

    tbd.write(dest, new ModListNode[V, Q](f(tbd, chunk), newNextMod))
  }

  def print = "ChunkListNode(" + chunk + ")"
}
