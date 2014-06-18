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

import scala.collection.mutable.Buffer

import tbd.{Changeable, TBD}
import tbd.Constants._
import tbd.memo.Lift

class DoubleChunkList[T, U](
    _head: Mod[DoubleChunkListNode[T, U]]) extends AdjustableChunkList[T, U] {
  val head = _head

  def map[V, Q](
      tbd: TBD,
      f: (TBD, (T, U)) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleChunkList[V, Q] = {
    if (parallel) {
      new DoubleChunkList(
        tbd.mod((dest: Dest[DoubleChunkListNode[V, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.parMap(tbd, dest, f)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    } else {
      val lift = tbd.makeLift[Mod[DoubleChunkListNode[V, Q]]](!memoized)
      new DoubleChunkList(
        tbd.mod((dest: Dest[DoubleChunkListNode[V, Q]]) => {
          tbd.read(head)(node => {
            if (node != null) {
              node.map(tbd, dest, f, lift)
            } else {
              tbd.write(dest, null)
            }
          })
        })
      )
    }
  }

  def chunkMap[V, Q](
      tbd: TBD,
      f: (TBD, Vector[(T, U)]) => (V, Q),
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleModList[V, Q] = {
    if (parallel || !memoized) {
      tbd.log.warning("DoubleChunkList.chunkMap ignores the 'parallel' and " +
		      "'memoized' parameters.")
    }

    val lift = tbd.makeLift[Mod[DoubleModListNode[V, Q]]]()
    new DoubleModList(
      tbd.mod((dest: Dest[DoubleModListNode[V, Q]]) => {
        tbd.read(head)(node => {
          if (node != null) {
            node.chunkMap(tbd, dest, f, lift)
          } else {
            tbd.write(dest, null)
          }
        })
      })
    )
  }

  def filter(
      tbd: TBD,
      pred: ((T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = true): DoubleChunkList[T, U] = ???

  def reduce(
      tbd: TBD,
      _identityMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean = false,
      memoized: Boolean = true): Mod[(T, U)] = {

    val identityMod = tbd.mod((dest: Dest[Vector[(T, U)]]) => {
      tbd.read(_identityMod)(_identityValue => {
	tbd.write(dest, Vector(_identityValue))
      })
    })

    // Each round we need a hasher and a lift, and we need to guarantee that the
    // same hasher and lift are used for a given round during change propagation,
    // even if the first mod of the list is deleted.
    class RoundLift {
      val lift = tbd.makeLift[(Hasher,
                               Lift[Mod[DoubleChunkListNode[T, U]]],
                               RoundLift)](!memoized)

      def getTuple() =
        lift.memo(List(), () =>
                  (new Hasher(2, 4),
                   tbd.makeLift[Mod[DoubleChunkListNode[T, U]]](!memoized),
                   new RoundLift()))
    }

    def randomReduceList(
        head: DoubleChunkListNode[T, U],
        round: Int,
        dest: Dest[(T, U)],
        roundLift: RoundLift): Changeable[(T, U)] = {
      val tuple = roundLift.getTuple()

      val halfListMod =
        tbd.mod((dest: Dest[DoubleChunkListNode[T, U]]) =>
          halfList(identityMod, head, round, tuple._1, tuple._2, dest))

      tbd.read(halfListMod)(halfList =>
        tbd.read(halfList.nextMod)(next =>
          if(next == null)
            tbd.read(halfList.chunkMod)(chunk =>
              tbd.write(dest, chunk.reduce(f(tbd, _, _))))
          else
            randomReduceList(halfList, round + 1, dest, tuple._3)))
    }

    def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
      hasher.hash(id.hashCode() ^ round) == 0
    }

    def halfList(
        acc: Mod[Vector[(T, U)]],
        head: DoubleChunkListNode[T, U],
        round: Int,
        hasher: Hasher,
        lift: Lift[Mod[DoubleChunkListNode[T, U]]],
        dest: Dest[DoubleChunkListNode[T, U]])
          : Changeable[DoubleChunkListNode[T, U]] = {
      val newAcc = tbd.mod((dest: Dest[Vector[(T, U)]]) =>
        tbd.read(acc)((acc) =>
	  tbd.read(head.chunkMod)(chunk =>
	    if (chunk.size > 0)
              tbd.write(dest, Vector(f(tbd, acc(0), chunk.reduce(f(tbd, _, _)))))
	    else
	      tbd.write(dest, Vector(acc(0))))))

      if(binaryHash(head.chunkMod.id, round, hasher)) {
        val newNext = lift.memo(List(head.nextMod, identityMod), () =>
	  tbd.mod((dest: Dest[DoubleChunkListNode[T, U]]) =>
	    tbd.read(head.nextMod)(next =>
	      if (next == null)
	        tbd.write(dest, null)
	      else
	        halfList(identityMod, next, round, hasher, lift, dest))))
        tbd.write(dest, new DoubleChunkListNode(newAcc, newNext))
      } else {
        tbd.read(head.nextMod)(next =>
	  if (next == null) {
	    val newNext = tbd.createMod[DoubleChunkListNode[T, U]](null)
            tbd.write(dest, new DoubleChunkListNode(newAcc, newNext))
	  } else {
	    halfList(newAcc, next, round, hasher, lift, dest)
	  }
        )
      }
    }

    val roundLift = new RoundLift()
    tbd.mod((dest: Dest[(T, U)]) =>
      tbd.read(head)(head =>
        if(head == null)
          tbd.read(identityMod)(identity =>
            tbd.write(dest, identity(0)))
        else
          randomReduceList(head, 0, dest, roundLift)))
  }


  def split(
      tbd: TBD,
      pred: (TBD, (T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false):
       (AdjustableList[T, U], AdjustableList[T, U]) = ???

  def sort(
      tbd: TBD,
      comperator: (TBD, (T, U), (T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): AdjustableList[T, U] = {

    type SVec = Vector[(T, U)]
    type SChunk = DoubleChunkListNode[T, U]

    val nullNode: Mod[SChunk] = tbd.createMod(null)
    val reuceInitial: Mod[(SChunk, U)] = tbd.createMod((null, null.asInstanceOf[U]))

    def sortMapper(tbd: TBD, values: SVec): (SChunk, U) = {
        def sortComperator(a: (T, U), b: (T, U)): Boolean = {
          comperator(tbd, a, b)
        }
        val sortedVector = values.sortWith(sortComperator)

        (new DoubleChunkListNode(
          tbd.createMod(sortedVector),
          nullNode),
        values(0)._2)
      }

    val listOfSortedChunks = this.chunkMap(tbd, sortMapper, parallel, memoized)

    def reducer(tbd: TBD, a: (SChunk, U), b: (SChunk, U)): (SChunk, U) = {

      if(a._1 == null)
        return b
      if(b._1 == null)
        return a

      val (next, value) = tbd.mod2((nextDest: Dest[SChunk], valDest: Dest[SVec]) => {
        tbd.read2(a._1.chunkMod, b._1.chunkMod)((avec, bvec) => {
          //Could we create a global constant for chunk size?
          //Or should we do some randomization here?
          val chunkSize = 8

          var result = Vector[(T, U)]()

          var ac = 0
          var bc = 0

          while(result.length < chunkSize &&
                avec.length > ac &&
                bvec.length > bc) {
            if(comperator(tbd, avec(ac), bvec(bc))) {
              result = result :+ avec(ac)
              ac += 1
            } else {
              result = result :+ bvec(bc)
              bc += 1
            }
          }

          //Could we clean this up?
          //Could we handle this better (especially towards the end?)
          if(ac == avec.length && bc == bvec.length) {
            tbd.read2(a._1.nextMod, b._1.nextMod)((anext, bnext) => {
              tbd.write(nextDest, reducer(tbd, (anext, a._2), (bnext, b._2))._1)
            })
          } else if(ac == avec.length) {
            tbd.read(a._1.nextMod)((anext) => {
              val newBNext = new DoubleChunkListNode(
                tbd.createMod(bvec.drop(bc)), b._1.nextMod)
              tbd.write(nextDest, reducer(tbd, (anext, a._2), (newBNext, b._2))._1)
            })
          } else if(bc == bvec.length) {
            tbd.read(b._1.nextMod)((bnext) => {
              val newANext = new DoubleChunkListNode(
                tbd.createMod(avec.drop(ac)), a._1.nextMod)
              tbd.write(nextDest, reducer(tbd, (newANext, a._2), (bnext, b._2))._1)
            })
          } else {
            val newBNext = new DoubleChunkListNode(
              tbd.createMod(bvec.drop(bc)), b._1.nextMod)
            val newANext = new DoubleChunkListNode(
              tbd.createMod(avec.drop(ac)), a._1.nextMod)
            tbd.write(nextDest, reducer(tbd, (newANext, a._2), (newBNext, b._2))._1)
          }

          tbd.write(valDest, result)
        })
      })

      (new DoubleChunkListNode(value, next), a._2)
    }

    val reductionResult = listOfSortedChunks.reduce(tbd, reuceInitial, reducer, parallel, memoized)

    val sHead = tbd.mod((dest: Dest[SChunk]) => {
      tbd.read(reductionResult)(reductionResult => {
        tbd.write(dest, reductionResult._1)
      })
    })

    new DoubleChunkList(sHead)
  }

  /*def randomReduce(
      tbd: TBD,
      initialValueMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean,
      memoized: Boolean): Mod[(T, U)] = {
    val zero = 0
    val halfLift = tbd.makeLift[Mod[DoubleChunkListNode[T, U]]](!memoized)

    val identityMod = tbd.mod((dest: Dest[Vector[(T, U)]]) => {
      tbd.read(initialValueMod)(initialValue => {
	tbd.write(dest, Vector(initialValue))
      })
    })

    tbd.mod((dest: Dest[(T, U)]) => {
      tbd.read(head)(h => {
        if(h == null) {
          tbd.read(initialValueMod)(initialValue =>
            tbd.write(dest, initialValue))
        } else {
          randomReduceList(tbd, identityMod,
                           h, head, zero, dest,
                           halfLift, f)
        }
      })
    })
  }

  val hasher = new Hasher(2, 8)
  def randomReduceList(
      tbd: TBD,
      identityMod: Mod[Vector[(T, U)]],
      head: DoubleChunkListNode[T, U],
      headMod: Mod[DoubleChunkListNode[T, U]],
      round: Int,
      dest: Dest[(T, U)],
      lift: Lift[Mod[DoubleChunkListNode[T, U]]],
      f: (TBD, (T, U), (T, U)) => (T, U)): Changeable[(T, U)] = {
    val halfListMod =
        tbd.mod((dest: Dest[DoubleChunkListNode[T, U]]) => {
          halfList(tbd, identityMod, identityMod,
                   head, round, new Hasher(2, 8),
                   lift, dest, f)
      })

    tbd.read(halfListMod)(halfList => {
      tbd.read(halfList.nextMod)(next => {
        if(next == null) {
          tbd.read(halfList.chunkMod)(chunk =>
            tbd.write(dest, chunk.reduce(f(tbd, _, _))))
        } else {
            randomReduceList(tbd, identityMod, halfList, halfListMod,
                             round + 1, dest,
                             lift, f)
        }
      })
    })
  }

  def binaryHash(id: ModId, round: Int, hasher: Hasher) = {
    hasher.hash(id.hashCode() ^ round) == 0
  }

  def halfList(
      tbd: TBD,
      identityMod: Mod[Vector[(T, U)]],
      acc: Mod[Vector[(T, U)]],
      head: DoubleChunkListNode[T, U],
      round: Int,
      hasher: Hasher,
      lift: Lift[Mod[DoubleChunkListNode[T, U]]],
      dest: Dest[DoubleChunkListNode[T, U]],
      f: (TBD, (T, U), (T, U)) => (T, U)): Changeable[DoubleChunkListNode[T, U]] = {
    val newAcc = tbd.mod((dest: Dest[Vector[(T, U)]]) =>
      tbd.read(acc)((acc) =>
	tbd.read(head.chunkMod)(chunk =>
	  if (chunk.size > 0)
            tbd.write(dest, Vector(f(tbd, acc(0), chunk.reduce(f(tbd, _, _)))))
	  else
	    tbd.write(dest, Vector(acc(0)))
	)))

    if(binaryHash(head.chunkMod.id, round, hasher)) {
      val newNext =
	tbd.mod((dest: Dest[DoubleChunkListNode[T, U]]) =>
	  tbd.read(head.nextMod)(next =>
	    if (next == null)
	      tbd.write(dest, null)
	    else
	      halfList(tbd, identityMod, identityMod,
		       next, round, hasher, lift, dest, f)))
      tbd.write(dest, new DoubleChunkListNode(newAcc, newNext))
    } else {
      tbd.read(head.nextMod)(next => {
	if (next == null) {
	  val newNext = tbd.createMod[DoubleChunkListNode[T, U]](null)
          tbd.write(dest, new DoubleChunkListNode(newAcc, newNext))
	} else {
	  halfList(tbd, identityMod, newAcc,
		   next, round, hasher, lift, dest, f)
	}
      })
    }
  }

  def reduce(
      tbd: TBD,
      initialValueMod: Mod[(T, U)],
      f: (TBD, (T, U), (T, U)) => (T, U),
      parallel: Boolean = false,
      memoized: Boolean = true): Mod[(T, U)] = {
    randomReduce(tbd, initialValueMod, f, parallel, memoized)
  }*/

  /* Meta functions */
  def toBuffer(): Buffer[U] = {
    val buf = Buffer[U]()
    var node = head.read()
    while (node != null) {
      buf ++= node.chunkMod.read().map(value => value._2)
      node = node.nextMod.read()
    }

    buf
  }

  override def toString: String = {
    head.toString
  }
}
