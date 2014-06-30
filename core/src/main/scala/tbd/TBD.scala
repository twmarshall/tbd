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

import language.experimental.macros
import reflect.macros.Context

import tbd.Constants._
import tbd.ddg.{Node, Timestamp}
import tbd.master.Main
import tbd.messages._
import tbd.mod.{Dest, Mod}
import tbd.worker.{Worker, Task}

object TBD {
  var id = 0
}

object macrodef {
  def readMacro[T, U <: Changeable[_]]
      (c: Context)(mod: c.Expr[Mod[T]])(reader: c.Expr[(T => U)]): c.Expr[U] = {
    import c.universe._
    import compat._
    val readFunc = c.Expr[U](Select(c.prefix.tree, TermName("readInternal")))

    c.Expr[U](Function(List(ValDef(mod.tree.symbol), ValDef(reader.tree.symbol)), readFunc.tree))
  }
}

class TBD(id: String, _worker: Worker) {
  import worker.context.dispatcher
  val worker = _worker
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
   *  tbd.mod((dest: Dest[AnyRef]) => {
   *    val a = tbd.createMod("Hello");
   *    val b = tbd.createMod(12);
   *    val c = tbd.createMod("Bla");
   *
   *    tbd.readN(a, b, c)(x => x match {
   *      case Seq(a:String, b:Int, c:String) => {
   *        println(a + b + c)
   *        tbd.write(dest, null)
   *      }
   *    })
   *  })
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


  def increment(mod: Mod[Int]): Mod[Int] = {
    this.mod((dest: Dest[Int]) =>
      read(mod)(mod =>
        write(dest, mod + 1)))
  }

  def read[T, U <: Changeable[_]](mod: Mod[T])(reader: T => U): U = macro macrodef.readMacro[T, U]

  def readInternal[T, U <: Changeable[_]](mod: Mod[T])(reader: T => U): U = {
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

  def write[T](dest: Dest[T], value: T): Changeable[T] = {
    val awaiting = dest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    val changeable = new Changeable(dest.mod)
    if (Main.debug) {
      val writeNode = worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                          currentParent)
      writeNode.endTime = worker.ddg.nextTimestamp(writeNode)
    }

    changeable
  }

  def writeNoDest[T](value: T): Changeable[T] = {
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

  def writeNoDest2[T, U](value: T, value2: U): Changeable2[T, U] = {
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

  def writeNoDestLeft[T, U](value: T, mod2: Mod[U]): Changeable2[T, U] = {
    if (mod2 != currentDest2.mod) {
      println("WARNING - mod parameter to writeNoDestLeft doesn't match " +
	      "currentDest2" + mod2 + " " + currentDest2.mod)
    }

    val awaiting = currentDest.mod.update(value)
    Await.result(Future.sequence(awaiting), DURATION)

    val changeable = new Changeable2(currentDest.mod, mod2)
    if (Main.debug) {
      val writeNode = worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                          currentParent)
      writeNode.mod2 = mod2.asInstanceOf[Mod[Any]]
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

    val changeable = new Changeable2(mod, currentDest2.mod)
    if (Main.debug) {
      val writeNode = worker.ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]],
                                          currentParent)
      writeNode.mod2 = currentDest2.mod
      writeNode.endTime = worker.ddg.nextTimestamp(writeNode)
    }

    changeable.asInstanceOf[Changeable2[T, U]]
  }

  def createMod[T](value: T): Mod[T] = {
    this.mod((dest: Dest[T]) => {
      write(dest, value)
    })
  }

  def mod2[T, V](initializer : (Dest[T], Dest[V]) => Changeable[V]):
        (Mod[T], Mod[V]) = {
    var first: Mod[T] = null
    var second: Mod[V] = null
    first = this.mod((first: Dest[T]) => {
      second = this.mod((second: Dest[V]) => {
        initializer(first, second);
        new Changeable[V](null)
      })
      new Changeable[T](null)
    })

    (first, second)
  }

  def mod[T](initializer: Dest[T] => Changeable[T]): Mod[T] = {
    val d = new Dest[T](worker.datastoreRef)
    initializer(d)
    d.mod
  }

  var currentDest: Dest[Any] = null
  def modNoDest[T](initializer: () => Changeable[T]): Mod[T] = {
    val oldCurrentDest = currentDest
    currentDest = new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]
    initializer()
    val mod = currentDest.mod
    currentDest = oldCurrentDest

    mod.asInstanceOf[Mod[T]]
  }

  var currentDest2: Dest[Any] = null
  def modNoDest2[T, U](initializer: () => Changeable2[T, U]): (Mod[T], Mod[U]) = {
    val oldCurrentDest = currentDest
    currentDest = new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]

    val oldCurrentDest2 = currentDest2
    currentDest2 = new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]

    initializer()

    val mod = currentDest.mod
    currentDest = oldCurrentDest
    val mod2 = currentDest2.mod
    currentDest2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
  }

  def modNoDestLeft[T, U](initializer: () => Changeable2[T, U]): (Mod[T], Mod[U]) = {
    val oldCurrentDest = currentDest
    currentDest = new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]

    initializer()

    val mod = currentDest.mod
    currentDest = oldCurrentDest
    val mod2 = currentDest2.mod

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
  }

  def modNoDestRight[T, U](initializer: () => Changeable2[T, U]): (Mod[T], Mod[U]) = {
    val oldCurrentDest2 = currentDest2
    currentDest2 = new Dest[T](worker.datastoreRef).asInstanceOf[Dest[Any]]

    initializer()

    val mod = currentDest.mod
    val mod2 = currentDest2.mod
    currentDest2 = oldCurrentDest2

    (mod.asInstanceOf[Mod[T]], mod2.asInstanceOf[Mod[U]])
  }

  var workerId = 0
  def par[T, U](one: TBD => T, two: TBD => U): Tuple2[T, U] = {
    val task1 =  new Task(((tbd: TBD) => one(tbd)))
    val workerProps1 =
      Worker.props(id + "-" + workerId, worker.datastoreRef, worker.self)
    val workerRef1 = worker.context.system.actorOf(workerProps1, id + "-" + workerId)
    workerId += 1
    val oneFuture = workerRef1 ? RunTaskMessage(task1)

    val task2 =  new Task(((tbd: TBD) => two(tbd)))
    val workerProps2 =
      Worker.props(id + "-" + workerId, worker.datastoreRef, worker.self)
    val workerRef2 = worker.context.system.actorOf(workerProps2, id + "-" + workerId)
    workerId += 1
    val twoFuture = workerRef2 ? RunTaskMessage(task2)

    worker.ddg.addPar(workerRef1, workerRef2, currentParent)

    val oneRet = Await.result(oneFuture, DURATION).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, DURATION).asInstanceOf[U]
    new Tuple2(oneRet, twoRet)
  }

  def updated(args: List[_]): Boolean = {
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

  var liftId = 0
  def makeLift[T](dummy:Boolean = false) = {
    if(dummy) {
      new DummyLift[T](this, 0)
    } else {
      liftId += 1
      new Lift[T](this, liftId)
    }
  }
}
