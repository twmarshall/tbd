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

import tbd.{Changeable, TBD}
import tbd.memo.Lift

// The default value of zero for size works because size is only ever
// accessed by the Modifier, which will set it appropriately.
class ChunkListNode[T, U](
    _chunk: Vector[(T, U)],
    _nextMod: Mod[ChunkListNode[T, U]],
    _size: Int = 0) {
  val chunk = _chunk
  val nextMod = _nextMod
  val size = _size

  def chunkMap[V, Q](
      tbd: TBD,
      dest: Dest[ModListNode[V, Q]],
      f: (TBD, Vector[(T, U)]) => (V, Q),
      lift: Lift[Mod[ModListNode[V, Q]]])
        : Changeable[ModListNode[V, Q]] = {
    val newNextMod = lift.memo(List(nextMod), () =>
      tbd.mod((dest: Dest[ModListNode[V, Q]]) =>
        tbd.read(nextMod)(next => {
          if (next != null && next.chunk.size > 0)
            next.chunkMap(tbd, dest, f, lift)
          else
            tbd.write(dest, null)
        })))

    tbd.write(dest, new ModListNode[V, Q](f(tbd, chunk), newNextMod))
  }

  def print = "ChunkListNode(" + chunk + ")"
}
