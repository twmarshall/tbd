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

import java.io.Serializable
import scala.collection.mutable.Map

import tbd._
import tbd.TBD._

object ModListNode {
  type ChangeableTuple[T, U] = (Changeable[ModListNode[T, U]], Changeable[ModListNode[T, U]])
}

class ModListNode[T, U] (
    var value: (T, U),
    val next: Mod[ModListNode[T, U]]
  ) extends Serializable {

  override def equals(obj: Any): Boolean = {
    if (!obj.isInstanceOf[ModListNode[T, U]]) {
      false
    } else {
      val that = obj.asInstanceOf[ModListNode[T, U]]
      that.value == value && that.next == next
    }
  }

  override def hashCode() = value.hashCode() * next.hashCode()

  def filter(
      pred: ((T, U)) => Boolean,
      memo: Memoizer[Mod[ModListNode[T, U]]])
     (implicit c: Context): Changeable[ModListNode[T, U]] = {
    def readNext = {
      read(next) {
	case null => write[ModListNode[T, U]](null)
	case next => next.filter(pred, memo)
      }
    }

    if (pred(value)) {
      val newNext = memo(List(next)) {
        mod {
	  readNext
        }
      }
      write(new ModListNode(value, newNext))
    } else {
      readNext
    }
  }

  def loopJoin[V](
      that: ModList[T, V],
      memo: Memoizer[Changeable[ModListNode[T, (U, V)]]])
     (implicit c: Context): Changeable[ModListNode[T, (U, V)]] = {
    val joinedValue = that.joinOne(value)

    read(joinedValue) {
      case null =>
	read(next) {
	  case null =>
	    write[ModListNode[T, (U, V)]](null)
	  case next =>
	    memo(next) {
	      next.loopJoin(that, memo)
	    }
	}
      case value =>
	val newNext = mod {
	  read(next) {
	    case null =>
	      write[ModListNode[T, (U, V)]](null)
	    case next =>
	      memo(next) {
		next.loopJoin(that, memo)
	      }
	  }
	}

	write(new ModListNode[T, (U, V)](value, newNext))
    }
  }

  def joinOne[V](
      thatValue: (T, V))
     (implicit c: Context): Changeable[(T, (V, U))] = {
    if (value._1 == thatValue._1) {
      write((value._1, (thatValue._2, value._2)))
    } else {
      read(next) {
	case null =>
	  write[(T, (V, U))](null)
	case node =>
	  node.joinOne(thatValue)
      }
    }
  }

  def map[V, W](
      f: ((T, U)) => (V, W),
      memo: Memoizer[Changeable[ModListNode[V, W]]])
     (implicit c: Context): Changeable[ModListNode[V, W]] = {
    val newNext = mod({
      read(next) {
	case null =>
	  write[ModListNode[V, W]](null)
	case next =>
          memo(next) {
            next.map(f, memo)
          }
      }
    }, next.id)

    write(new ModListNode[V, W](f(value), newNext))
  }

  def merge(
      that: ModListNode[T, U],
      comparator: ((T, U), (T, U)) => Boolean,
      memo: Memoizer[Changeable[ModListNode[T, U]]],
      modizer: Modizer[ModListNode[T, U]])
     (implicit c: Context): Changeable[ModListNode[T, U]] = {
    if (comparator(value, that.value)) {
      val newNext =
	modizer(value) {
	read(next) {
	  case null =>
	    memo(null, that) {
	      that.mergeTail(memo, modizer)
	    }
	  case node =>
            memo(node, that) {
              node.merge(that, comparator, memo, modizer)
            }
	}
      }

      write(new ModListNode(value, newNext))
    } else {
      val newNext =
	modizer(that.value) {
	  read(that.next) {
	    case null =>
	      memo(null, this) {
		this.mergeTail(memo, modizer)
	      }
	    case node =>
              memo(this, node) {
		this.merge(node, comparator, memo, modizer)
              }
	}
      }

      write(new ModListNode(that.value, newNext))
    }
  }

  private def mergeTail(
      memo: Memoizer[Changeable[ModListNode[T, U]]],
      modizer: Modizer[ModListNode[T, U]])
     (implicit c: Context): Changeable[ModListNode[T, U]] = {
    val newNext = modizer(value) {
      read(next) {
	case null =>
	  write[ModListNode[T, U]](null)
	case next =>
          memo(next) {
            next.mergeTail(memo, modizer)
          }
      }
    }

    write(new ModListNode[T, U](value, newNext))
  }

  def sort(
      toAppend: Mod[ModListNode[T, U]],
      comparator: ((T, U), (T, U)) => Boolean,
      memoizers: Map[(T, U), Memoizer[ModListNode.ChangeableTuple[T, U]]],
      memo: Memoizer[Mod[ModListNode[T, U]]],
      memo2: Memoizer[Changeable[ModListNode[T, U]]])
     (implicit c: Context): Changeable[ModListNode[T, U]] = {
    val (smaller, greater) = mod2 {
      if (!memoizers.contains(value)) {
	memoizers(value) = makeMemoizer[ModListNode.ChangeableTuple[T, U]]()
      }

      val memoSplit = memoizers(value)
      read_2(next) {
	case null =>
	  write2[ModListNode[T, U], ModListNode[T, U]](null, null)
        case nextNode =>
	  memoSplit(nextNode) {
	    nextNode.split(memoSplit, (cv: (T, U)) => { comparator(cv, value) })
	  }
      }
    }

    val greaterSorted = memo(greater) {
      mod {
        read(greater) {
	  case null =>
	    read(toAppend) { write(_) }
	  case greater =>
	    greater.sort(toAppend, comparator, memoizers, memo, memo2)
        }
      }
    }

    memo2(smaller) {
      val mid = new ModListNode(value, greaterSorted)

      read(smaller) {
	case null =>
	  write(mid)
	case smallerNode =>
	  smallerNode.sort(createMod(mid), comparator, memoizers, memo, memo2)
      }
    }
  }

  def split(
      memo: Memoizer[ModListNode.ChangeableTuple[T, U]],
      pred: ((T, U)) => Boolean)
     (implicit c: Context): ModListNode.ChangeableTuple[T, U] = {
    def readNext(next: Mod[ModListNode[T, U]]) = {
      read_2(next) {
        case null =>
	  write2[ModListNode[T, U], ModListNode[T, U]](null, null)
        case next =>
          memo(next) {
	    next.split(memo, pred)
          }
      }
    }

    if(pred(value)) {
      val (matchMod, diffChangeable) =
	modLeft({
	  readNext(next)
	}, next.id)

      writeLeft(new ModListNode(value, matchMod), diffChangeable)
    } else {
      val (matchChangeable, diffMod) =
	modRight({
	  readNext(next)
	}, next.id)

      writeRight(matchChangeable, new ModListNode(value, diffMod))
    }
  }

  override def toString = "Node(" + value + ", " + next + ")"
}
