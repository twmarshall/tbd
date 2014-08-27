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
package tbd.list

import scala.collection.mutable.Buffer

import tbd._
import tbd.Constants.ModId
import tbd.TBD._

class ChunkList[T, U](
    val head: Mod[ChunkListNode[T, U]],
    conf: ListConf) extends AdjustableList[T, U] {

  override def chunkMap[V, W](
      f: (Vector[(T, U)]) => (V, W))
     (implicit c: Context): ModList[V, W] = {
    val memo = makeMemoizer[Mod[ModListNode[V, W]]]()

    new ModList(
      mod {
        read(head) {
	  case null => write[ModListNode[V, W]](null)
	  case node => node.chunkMap(f, memo)
        }
      }
    )
  }

  def filter(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): ChunkList[T, U] = ???

  def flatMap[V, W](
      f: ((T, U)) => Iterable[(V, W)])
     (implicit c: Context): ChunkList[V, W] = {
    val memo = makeMemoizer[Changeable[ChunkListNode[V, W]]]()

    new ChunkList(
      mod {
        read(head) {
	  case null => write[ChunkListNode[V, W]](null)
	  case node => node.flatMap(f, conf.chunkSize, memo)
        }
      }, conf
    )
  }

  def join[V](
      _that: AdjustableList[T, V])
     (implicit c: Context): ChunkList[T, (U, V)] = {
    assert(_that.isInstanceOf[ChunkList[T, V]])
    val that = _that.asInstanceOf[ChunkList[T, V]]

    val memo = makeMemoizer[Changeable[ChunkListNode[T, (U ,V)]]]()

    new ChunkList(
      mod {
	read(head) {
	  case null => write[ChunkListNode[T, (U, V)]](null)
	  case node => node.loopJoin(that, memo)
	}
      }, conf
    )
  }

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
      }, conf
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
      }, conf
    )
  }

  def reduce(
      f: ((T, U), (T, U)) => (T, U))
     (implicit c: Context): Mod[(T, U)] = ???

  def reduceByKey(
      f: (U, U) => U,
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): ChunkList[T, U] = {
    val sorted = this.sort(comparator)

    new ChunkList(
      mod {
	read(sorted.head) {
	  case null =>
	    write(null)
	  case node =>
	    node.reduceByKey(f, node.chunk.head._1, null.asInstanceOf[U])
	}
      }, conf
    )
  }

  def sort(
      comparator: ((T, U), (T, U)) => Boolean)
     (implicit c: Context): ChunkList[T, U] = {
    def mapper(chunk: Vector[(T, U)]) = {
      val tail = mod({
	write(new ChunkListNode[T, U]((chunk.toBuffer.sortWith(comparator).toVector), mod({ write(null) })))
      })

      ("", new ChunkList(tail, conf))
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
      }, conf
    )
  }

  def split(
      pred: ((T, U)) => Boolean)
     (implicit c: Context): (ChunkList[T, U], ChunkList[T, U]) = ???

  /* Meta functions */
  def toBuffer(): Buffer[(T, U)] = {
    val buf = Buffer[(T, U)]()
    var node = head.read()
    while (node != null) {
      buf ++= node.chunk
      node = node.nextMod.read()
    }

    buf
  }

  override def toString = head.toString
}
