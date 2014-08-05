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

import tbd.{Changeable, Context, Memoizer, Modizer}
import tbd.Constants.ModId
import tbd.TBD._

class ChunkList[T, U](
    val head: Mod[ChunkListNode[T, U]]) extends AdjustableChunkList[T, U] {

  def map[V, W](
      f: ((T, U)) => (V, W))
     (implicit c: Context): ChunkList[V, W] = {
    val memo = makeMemoizer[Changeable[ChunkListNode[V, W]]]()
    new ChunkList(
      mod {
        read(head) {
	  case null => write[ChunkListNode[V, W]](null)
	  case node => node.map(f, memo)
        }
      }
    )
  }

  def merge(
      that: ChunkList[T, U],
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): ChunkList[T, U] = {
    merge(that, comparator, makeMemoizer[Changeable[ChunkListNode[T, U]]](), makeModizer[ChunkListNode[T, U]]())
  }

  def merge(
      that: ChunkList[T, U],
      comparator: ((T, U), (T, U)) => Boolean,
      memo: Memoizer[Changeable[ChunkListNode[T, U]]],
      modizer: Modizer[ChunkListNode[T, U]])
     (implicit c: Context): ChunkList[T, U] = {

    def innerMerge(
        one: ChunkListNode[T, U],
        two: ChunkListNode[T, U],
        comparator: ((T, U), (T, U)) => Boolean,
        _oneRemaining: Vector[(T, U)],
        _twoRemaining: Vector[(T, U)],
        memo: Memoizer[Changeable[ChunkListNode[T, U]]],
        modizer: Modizer[ChunkListNode[T, U]])
       (implicit c: Context): Changeable[ChunkListNode[T, U]] = {
      val oneRemaining =
	if (one == null)
	  _oneRemaining
	else
	  _oneRemaining ++ one.chunk

      val twoRemaining =
	if (two == null)
	  _twoRemaining
	else
	  _twoRemaining ++ two.chunk

      var i = 0
      var j = 0
      var newChunk = Vector[(T, U)]()
      while (i < oneRemaining.size && j < twoRemaining.size) {
	if (comparator(oneRemaining(i), twoRemaining(j))) {
	  newChunk :+= oneRemaining(i)
	  i += 1
	} else {
	  newChunk :+= twoRemaining(j)
	  j += 1
	}
      }

      val newOneRemaining = oneRemaining.drop(i)
      val newTwoRemaining = twoRemaining.drop(j)

      val newNext = mod {
	if (one == null) {
	  if (two == null) {
	      write(new ChunkListNode(newOneRemaining ++ newTwoRemaining, createMod[ChunkListNode[T, U]](null)))
	  } else {
	    read(two.nextMod) {
	      case twoNode =>
		memo(null, twoNode, newOneRemaining, newTwoRemaining) {
		  innerMerge(null, twoNode, comparator, newOneRemaining, newTwoRemaining, memo, modizer)
		}
	    }
	  }
	} else {
	  read(one.nextMod) {
	    case oneNode =>
	      if (two == null) {
		memo(oneNode, null, newOneRemaining, newTwoRemaining) {
		  innerMerge(oneNode, null, comparator, newOneRemaining, newTwoRemaining, memo, modizer)
		}
	      } else {
		read(two.nextMod) {
		  case twoNode =>
		    memo(oneNode, twoNode, newOneRemaining, newTwoRemaining) {
		      innerMerge(oneNode, twoNode, comparator, newOneRemaining, newTwoRemaining, memo, modizer)
		    }
		}
	      }
	  }
	}
      }

      write(new ChunkListNode(newChunk, newNext))
    }

    new ChunkList(
      mod {
	read(head) {
	  case null => read(that.head) { write(_) }
	  case node =>
	    read(that.head) {
	      case null => write(node)
	      case thatNode =>
		memo(node, thatNode) {
		  innerMerge(node, thatNode, comparator, Vector[(T, U)](), Vector[(T, U)](), memo, modizer)
		}
	    }
	}
      }
    )
  }

  def chunkMap[V, Q](
      f: (Vector[(T, U)]) => (V, Q))
     (implicit c: Context): ModList[V, Q] = {
    val memo = makeMemoizer[Mod[ModListNode[V, Q]]]()
    new ModList(
      mod {
        read(head) {
	  case null => write[ModListNode[V, Q]](null)
	  case node => node.chunkMap(f, memo)
        }
      }
    )
  }

  def filter(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): ChunkList[T, U] = ???

  def reduce(
      f: ((T, U), (T, U)) => (T, U))
     (implicit c: Context): Mod[(T, U)] = ???

  def split(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): (AdjustableList[T, U], AdjustableList[T, U]) = ???

  def sort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): AdjustableList[T, U] = {
    def mapper(chunk: Vector[(T, U)]) = {
      val tail = mod({
	write(new ChunkListNode[T, U]((chunk.toBuffer.sortWith(comparator).toVector), mod({ write(null) })))
      })

      ("", new ChunkList(tail))
    }

    val memo = makeMemoizer[(Memoizer[Changeable[ChunkListNode[T, U]]],
			     Modizer[ChunkListNode[T, U]])]()

    def reducer(pair1: (String, ChunkList[T, U]), pair2: (String, ChunkList[T, U])) = {
      val (memoizer, modizer) = memo(pair1, pair2) {
        (makeMemoizer[Changeable[ChunkListNode[T, U]]](),
	 makeModizer[ChunkListNode[T, U]]())
      }

      val merged = pair2._2.merge(pair1._2, comparator, memoizer, modizer)

      (pair1._1 + pair2._1, merged)
    }

    val mapped = chunkMap(mapper)
    val reduced = mapped.reduce(reducer)

    new ChunkList(
      mod {
        read(reduced) {
          case (key, list) => read(list.head) { write(_) }
        }
      }
    )
  }

  def chunkSort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): Mod[(Int, Array[(T, U)])] = {
    def mapper(chunk: Vector[(T, U)]): (Int, Array[(T, U)]) = {
      (0, chunk.sortWith(comparator).toArray)
    }
    val sortedChunks = chunkMap(mapper)

    def reducer(pair1: (Int, Array[(T, U)]), pair2: (Int, Array[(T, U)])) = {
      val array1 = pair1._2
      val array2 = pair2._2

      val reduced = new Array[(T, U)](array1.size + array2.size)

      var i = 0
      var j = 0
      while (i < array1.size && j < array2.size) {
	if (comparator(array1(i), array2(j))) {
	  reduced(i + j) = array1(i)
	  i += 1
	} else {
	  reduced(i + j) = array2(j)
	  j += 1
	}
      }

      while (i < array1.size) {
	reduced(i + j) = array1(i)
	i += 1
      }

      while (j < array2.size) {
	reduced(i + j) = array2(j)
	j += 1
      }

      (pair1._1, reduced)
    }

    sortedChunks.reduce(reducer)
  }

  /* Meta functions */
  def toBuffer(): Buffer[U] = {
    val buf = Buffer[U]()
    var node = head.read()
    while (node != null) {
      buf ++= node.chunk.map(value => value._2)
      node = node.nextMod.read()
    }

    buf
  }

  override def toString = head.toString
}
