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
package tbd

import akka.pattern.ask
import akka.actor.ActorRef
import akka.event.Logging
import scala.collection.mutable.{ArrayBuffer, ListBuffer, Map, Set}
import scala.concurrent.{Await, Future, Promise}

import tbd.Constants._
import tbd.ddg.{MemoNode, Node, Timestamp}
import tbd.master.{Main, Master}
import tbd.messages._
import tbd.mod.{Dest, Mod}
import tbd.worker.Worker

class Parer[T, U](value1: Context => T) {
  def and(value2: Context => U)(implicit c: Context) = {
    c.par(value1, value2)
  }
}

object TBD {
  def read[T, U <: Changeable[_]](mod: Mod[T])(reader: T => U)(implicit c: Context): U = {
    c.read(mod)(reader)
  }

  def read2[T, V, U](
      a: Mod[T],
      b: Mod[V])
     (reader: (T, V) => (Changeable[U]))
     (implicit c: Context): Changeable[U] = {
    c.read2(a, b)(reader)
  }

  def mod[T](initializer: => Changeable[T])(implicit c: Context): Mod[T] = {
    c.mod(initializer)
  }

  def mod2[T, U](
      write: Int)
     (initializer: => Changeable2[T, U])
     (implicit c: Context): (Mod[T], Mod[U]) = {
    c.mod_2(write)(initializer)
  }

  def write[T](value: T)(implicit c: Context): Changeable[T] = {
    c.write(value)
  }

  def write2[T, U](value: T, value2: U)(implicit c: Context): Changeable2[T, U] = {
    c.write2(value, value2)
  }

  def par[T, U](one: Context => T): Parer[T, U] = {
    new Parer(one)
  }

  def makeMemoizer[T](dummy: Boolean = false)(implicit c: Context): Memoizer[T] = {
    c.makeMemoizer[T](dummy)
  }

  def createMod[T](value: T)(implicit c: Context): Mod[T] = {
    c.createMod(value)
  }
}

class Context(id: String, val worker: Worker) {
  import worker.context.dispatcher
  var initialRun = true

  // The Node representing the currently executing reader.
  var currentParent: Node = worker.ddg.root

  val log = Logging(worker.context.system, "TBD" + id)

  // Contains a list of mods that have been updated since the last run of change
  // propagation, to determine when memo matches can be made.
  val updatedMods = Set[ModId]()

  // The timestamp of the read currently being reexecuting during change
  // propagation.
  var reexecutionStart: Timestamp = null

  // The timestamp of the node immediately after the end of the read being
  // reexecuted.
  var reexecutionEnd: Timestamp = null

  def read2[T, V, U](a: Mod[T], b: Mod[V])
                    (reader: (T, V) => (Changeable[U])): Changeable[U] = {
    read(a)((a) => {
      read(b)((b) => reader(a, b))
    })
  }

  /* readN - Read n mods. Experimental function.
   *
   * Usage Example:
   *
   *  mod {
   *    val a = createMod("Hello");
   *    val b = createMod(12);
   *    val c = createMod("Bla");
   *
   *    readN(a, b, c)(x => x match {
   *      case Seq(a:String, b:Int, c:String) => {
   *        println(a + b + c)
   *        write(dest, null)
   *      }
   *    })
   *  }
   */
  def readN[U](args: Mod[U]*)
              (reader: (Seq[_]) => (Changeable[U])) : Changeable[U] = {

    readNHelper(args, ListBuffer(), reader)
  }

  private def readNHelper[U](mods: Seq[Mod[_]],
                     values: ListBuffer[AnyRef],
                     reader: (Seq[_]) => (Changeable[U])) : Changeable[U] = {
    val tail = mods.tail
    val head = mods.head


    read(head)((value) => {
      values += value.asInstanceOf[AnyRef]
      if(tail.isEmpty) {
        reader(values.toSeq)
      } else {
        readNHelper(tail, values, reader)
      }
    })
  }

  def read[T, U <: Changeable[_]](mod: Mod[T])(reader: T => U): U = {
    val readNode = worker.ddg.addRead(mod.asInstanceOf[Mod[Any]],
                                      currentParent,
                                      reader.asInstanceOf[Any => Changeable[Any]])

    val outerReader = currentParent
    currentParent = readNode

    val value = mod.read(worker.self)

    val changeable = reader(value)
    currentParent = outerReader

    readNode.endTime = worker.ddg.nextTimestamp(readNode)
    readNode.currentDest = currentDest
    readNode.currentDest2 = currentDest2

    changeable
  }

  def write[T](value: T): Changeable[T] = {
    val awaiting = currentDest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    val changeable = new Changeable(currentDest.mod)
    if (Main.debug) {
      val writeNode = worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                          currentParent)
      writeNode.endTime = worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable[T]]
  }

  def write2[T, U](value: T, value2: U): Changeable2[T, U] = {
    val awaiting = currentDest.mod.update(value)
    val awaiting2 = currentDest2.mod.update(value2)
    Await.result(Future.sequence(awaiting), DURATION)
    Await.result(Future.sequence(awaiting2), DURATION)

    val changeable = new Changeable2(currentDest.mod, currentDest2.mod)
    if (Main.debug) {
      val writeNode = worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                          currentParent)
      writeNode.mod2 = changeable.mod2
      writeNode.endTime = worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable2[T, U]]
  }

  def writeNoDestLeft[T, U](value: T, mod: Mod[U]): Changeable2[T, U] = {
    if (mod != currentDest2.mod) {
      println("WARNING - mod parameter to write2(0) doesn't match " +
	      "currentDest2")
    }

    val awaiting = currentDest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    val changeable = new Changeable2(currentDest.mod, currentDest2.mod)
    if (Main.debug) {
      val writeNode = worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                          currentParent)
      writeNode.mod2 = currentDest2.mod
      writeNode.endTime = worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable2[T, U]]
  }

  def writeNoDestRight[T, U](mod: Mod[T], value2: U): Changeable2[T, U] = {
    if (mod != currentDest.mod) {
      println("WARNING - mod parameter to writeNoDestRight doesn't match " +
	      "currentDest " + mod + " " + currentDest.mod)
    }

    val awaiting = currentDest2.mod.update(value2)
    Await.result(Future.sequence(awaiting), DURATION)

    val changeable = new Changeable2(currentDest.mod, currentDest2.mod)
    if (Main.debug) {
      val writeNode = worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                          currentParent)
      writeNode.mod2 = currentDest2.mod
      writeNode.endTime = worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable2[T, U]]
  }

  def createMod[T](value: T): Mod[T] = {
    this.mod {
      write(value)
    }
  }
  
  var currentDest: Dest[Any] = null
  def mod[T](initializer: => Changeable[T]): Mod[T] = {
    val oldCurrentDest = currentDest
    currentDest = new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]
    initializer
    val mod = currentDest.mod
    currentDest = oldCurrentDest

    mod.asInstanceOf[Mod[T]]
  }

  var currentDest2: Dest[Any] = null
  def mod_2[T, U](write: Int)(initializer: => Changeable2[T, U]): (Mod[T], Mod[U]) = {
    val oldCurrentDest = currentDest
    currentDest =
      if (write == 0 || write == 2)
	new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]
      else
	currentDest

    val oldCurrentDest2 = currentDest2
    currentDest2 =
      if (write == 1 || write == 2)
	new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]
      else
	currentDest2

    initializer

    val mod = currentDest.mod
    currentDest = oldCurrentDest
    val mod2 = currentDest2.mod
    currentDest2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
  }

  var workerId = 0
  def par[T, U](one: Context => T, two: Context => U): (T, U) = {
    val workerProps1 =
      Worker.props(id + "-" + workerId, worker.datastoreRef, worker.self)
    val workerRef1 = worker.context.system.actorOf(workerProps1, id + "-" + workerId)
    workerId += 1

    val adjust1 = new Adjustable[T] { def run(implicit c: Context) = one(c) }
    val oneFuture = workerRef1 ? RunTaskMessage(adjust1)

    val workerProps2 =
      Worker.props(id + "-" + workerId, worker.datastoreRef, worker.self)
    val workerRef2 = worker.context.system.actorOf(workerProps2, id + "-" + workerId)
    workerId += 1

    val adjust2 = new Adjustable[U] { def run(implicit c: Context) = two(c) }
    val twoFuture = workerRef2 ? RunTaskMessage(adjust2)

    worker.ddg.addPar(workerRef1, workerRef2, currentParent)

    val oneRet = Await.result(oneFuture, DURATION).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, DURATION).asInstanceOf[U]
    new Tuple2(oneRet, twoRet)
  }

  def updated(args: Seq[_]): Boolean = {
    var updated = false

    for (arg <- args) {
      if (arg.isInstanceOf[Mod[_]]) {
        if (updatedMods.contains(arg.asInstanceOf[Mod[_]].id)) {
	  updated = true
        }
      }
    }

    updated
  }

  var nextMemoId = 0
  def makeMemoizer[T](dummy:Boolean = false) = {
    nextMemoId += 1

    if(dummy) {
      new DummyMemoizer[T](this, nextMemoId)
    } else {
      new Memoizer[T](this, nextMemoId)
    }
  }
}
