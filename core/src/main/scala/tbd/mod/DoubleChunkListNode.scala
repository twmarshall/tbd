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
class DoubleChunkListNode[T, U](
    _chunkMod: Mod[Vector[(T, U)]],
    _nextMod: Mod[DoubleChunkListNode[T, U]],
    _size: Int = 0) {
  val chunkMod = _chunkMod
  val nextMod = _nextMod
  val size = _size

  def map[V, Q](
      tbd: TBD,
      dest: Dest[DoubleChunkListNode[V, Q]],
      f: (TBD, (T, U)) => (V, Q),
      lift: Lift[Mod[DoubleChunkListNode[V, Q]]])
        : Changeable[DoubleChunkListNode[V, Q]] = {
    val newChunkMod = tbd.mod((dest: Dest[Vector[(V, Q)]]) =>
      tbd.read(chunkMod)(chunk =>
        tbd.write(dest, chunk.map(pair => f(tbd, pair)))))
    val newNextMod = lift.memo(List(nextMod), () =>
      tbd.mod((dest: Dest[DoubleChunkListNode[V, Q]]) =>
        tbd.read(nextMod)(next =>
          if (next != null)
            next.map(tbd, dest, f, lift)
          else
            tbd.write(dest, null))))

    tbd.write(dest, new DoubleChunkListNode[V, Q](newChunkMod, newNextMod))
  }

  def chunkMap[V, Q](
      tbd: TBD,
      dest: Dest[DoubleModListNode[V, Q]],
      f: (TBD, Vector[(T, U)]) => (V, Q),
      lift: Lift[Mod[DoubleModListNode[V, Q]]])
        : Changeable[DoubleModListNode[V, Q]] = {
    val newChunkMod = tbd.mod((dest: Dest[(V, Q)]) =>
      tbd.read(chunkMod)(chunk =>
        tbd.write(dest, f(tbd, chunk))))
    val newNextMod = lift.memo(List(nextMod), () =>
      tbd.mod((dest: Dest[DoubleModListNode[V, Q]]) =>
        tbd.read(nextMod)(next =>
          if (next != null)
            next.chunkMap(tbd, dest, f, lift)
          else
            tbd.write(dest, null))))

    tbd.write(dest, new DoubleModListNode[V, Q](newChunkMod, newNextMod))
  }

  def parMap[V, Q](
      tbd: TBD,
      dest: Dest[DoubleChunkListNode[V, Q]],
      f: (TBD, (T, U)) => (V, Q)): Changeable[DoubleChunkListNode[V, Q]] = {
    val parTuple = tbd.par(
      (tbd: TBD) =>
        tbd.mod((dest: Dest[Vector[(V, Q)]]) =>
          tbd.read(chunkMod)(chunk =>
            tbd.write(dest, chunk.map(pair => f(tbd, pair))))),
      (tbd: TBD) =>
        tbd.mod((dest: Dest[DoubleChunkListNode[V, Q]]) =>
          tbd.read(nextMod)(next =>
            if (next != null)
              next.parMap(tbd, dest, f)
            else
              tbd.write(dest, null))))

    tbd.write(dest, new DoubleChunkListNode[V, Q](parTuple._1, parTuple._2))
  }

  override def toString: String = {
    def toString(lst: DoubleChunkListNode[T, U]): String = {
      val nextRead = lst.nextMod.read()
      val next =
	if (nextRead != null)
	  ", " + toString(nextRead)
	else
	  ")"

      lst.chunkMod + next
    }

    "DoubleModListNode(" + toString(this)
  }
}
