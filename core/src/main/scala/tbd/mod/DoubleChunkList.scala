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

  ///Weeeeelll, ther we go with sort.
  def sort(
      tbd: TBD,
      comperator: (TBD, (T, U), (T, U)) => Boolean,
      parallel: Boolean = false,
      memoized: Boolean = false): AdjustableList[T, U] = {

    type SVec = Vector[(T, U)]
    type SChunk = DoubleChunkListNode[T, U]

    val nullNode: Mod[SChunk] = tbd.createMod(null)
    val reuceInitial: Mod[(Int, SChunk)] = tbd.createMod((0, null))

    def sortMapper(tbd: TBD, values: SVec): (SVec) = {
      def sortComperator(a: (T, U), b: (T, U)): Boolean = {
        comperator(tbd, a, b)
      }

      values.sortWith(sortComperator)
    }

    val rand = new scala.util.Random()

    def customChunkMap (
        node: SChunk,
        tbd: TBD,
        dest: Dest[DoubleModListNode[Int, SChunk]],
        lift: Lift[Mod[DoubleModListNode[Int, SChunk]]])
          : Changeable[DoubleModListNode[Int, SChunk]] = {
      val newChunkMod = tbd.mod((destNode: Dest[(Int, SChunk)]) => {
        val newContent = tbd.mod((destContent: Dest[SVec]) => {
          tbd.read(node.chunkMod)(chunk => {
            tbd.write(destContent, sortMapper(tbd, chunk))
          })
        })
        //It should be safe to use random keys here -
        //if we re-create a DML node, we got a major structure
        //change anyway.
        tbd.write(destNode, (rand.nextInt, new DoubleChunkListNode(newContent, nullNode))) //Create meaningfull key here.
      })
      val newNextMod = lift.memo(List(node.nextMod), () =>
        tbd.mod((dest: Dest[DoubleModListNode[Int, SChunk]]) =>
          tbd.read(node.nextMod)(next =>
            if (next != null)
              customChunkMap(next, tbd, dest, lift)
            else
              tbd.write(dest, null))))
      tbd.write(dest, new DoubleModListNode[Int, SChunk](newChunkMod, newNextMod))
    }

    val chunkMapLift = tbd.makeLift[Mod[DoubleModListNode[Int, SChunk]]](!memoized)

    val listOfSortedChunks = new DoubleModList(tbd.mod((dest: Dest[DoubleModListNode[Int, SChunk]]) => {
      tbd.read(head)(head => {
        customChunkMap(head, tbd, dest, chunkMapLift)
      })
    }))

    val lift = tbd.makeLift[SChunk](!memoized)

    def reducer(nextDest: Dest[SChunk], valDest: Dest[SVec], tbd: TBD, a: (Int, SChunk), b: (Int, SChunk), akku: SVec): Changeable[SVec] = {
      if(a._2 == null || b._2 == null) {
        tbd.write(nextDest, if(a._2 == null) b._2 else a._2)
        tbd.write(valDest, akku)
      } else {
        tbd.read2(a._2.chunkMod, b._2.chunkMod)((avec, bvec) => {
          //Could we create a global constant for chunk size?
          //Or should we do some randomization here?
          //Or does the mutator tell us the size magically?
          val expectedChunkSize = 16

          var result = akku

          var ac = 0
          var bc = 0

          var endOfAReached = avec.length == 0
          var endOfBReached = bvec.length == 0
          var shouldCutResult = false

          while(!endOfAReached &&
                !endOfBReached &&
                !shouldCutResult) { // 1
            if(comperator(tbd, avec(ac), bvec(bc))) {
              result = result :+ avec(ac)
              ac += 1
            } else {
              result = result :+ bvec(bc)
              bc += 1
            }

            endOfAReached = avec.length <= ac
            endOfBReached = bvec.length <= bc
            shouldCutResult = result.last._1.hashCode % expectedChunkSize == 0
          }

          if(shouldCutResult) {
            val newb = bvec.drop(bc)
            val newa = avec.drop(ac)
            tbd.write(nextDest, //Where does nextDest come from? We want to omit a write...
              lift.memo(List(b._2.nextMod, newb, a._2.nextMod, //Repro a similar memo issue.
                           newa, a._1, b._1), () => {
              val newBNext = new DoubleChunkListNode(
                tbd.createMod(newb), b._2.nextMod)
              val newANext = new DoubleChunkListNode(
                tbd.createMod(newa), a._2.nextMod)
              reduceWrapper(tbd, (a._1, newANext), (b._1, newBNext))._2
            }))
            tbd.write(valDest, result)
          } else {
            if(endOfAReached && endOfBReached) {
              tbd.read2(a._2.nextMod, b._2.nextMod)((anext, bnext) => {
                reducer(nextDest, valDest, tbd, (a._1, anext), (b._1, bnext), result)
              })
            } else if(endOfAReached) {
              tbd.read(a._2.nextMod)((anext) => {
                val newBNext = new DoubleChunkListNode(
                  tbd.createMod(bvec.drop(bc)), b._2.nextMod)
                  reducer(nextDest, valDest, tbd, (a._1, anext), (b._1, newBNext), result)
              })
            } else { //endOfBReached
              tbd.read(b._2.nextMod)((bnext) => {
                val newANext = new DoubleChunkListNode(
                  tbd.createMod(avec.drop(ac)), a._2.nextMod)
                  reducer(nextDest, valDest, tbd, (a._1, newANext), (b._1, bnext), result)
              })
            }
          }
        })
      }
    }

    def reduceWrapper (tbd: TBD, a: (Int, SChunk), b: (Int, SChunk)): (Int, SChunk) = {
      if(a._2 == null)
        return b
      if(b._2 == null)
        return a

      val (next, value) = tbd.mod2((nextDest: Dest[SChunk],
                valDest: Dest[SVec]) => {
        reducer(nextDest, valDest, tbd, a, b, Vector[(T, U)]())
      })

      //Well, here is my problem...
      (a._1, new DoubleChunkListNode(value, next))
    }

    val reductionResult = listOfSortedChunks.reduce(tbd, reuceInitial,
                                                    reduceWrapper, parallel, memoized)

    val sHead = tbd.mod((dest: Dest[SChunk]) => {
      tbd.read(reductionResult)(reductionResult => {
        tbd.write(dest, reductionResult._2)
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
