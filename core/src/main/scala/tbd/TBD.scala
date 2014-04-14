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
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import scala.collection.mutable.{Map, Set}
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import tbd.ddg.{DDG, Node}
import tbd.memo.{Lift, MemoEntry}
import tbd.messages._
import tbd.mod.{Mod, ModId}
import tbd.worker.{Worker, Task}

object TBD {
  var id = 0
}

class TBD(
    id: String,
    ddg: DDG,
    datastoreRef: ActorRef,
    workerRef: ActorRef,
    system: ActorSystem) {

  var initialRun = true

  // The Node representing the currently executing reader.
  var currentParent: Node = ddg.root
  val input = new Reader(datastoreRef)

  val log = Logging(system, "TBD" + id)

  implicit val timeout = Timeout(300 seconds)

  // Represents the number of PebblingFinishedMessages this worker is waiting
  // on before it can finish.
  var awaiting = 0

  // Maps ModIds to the mod's value for LocalMods created in this TBD.
  val mods = scala.collection.mutable.Map[ModId, Any]()

  // Maps modIds to the workers that have read the corresponding mod.
  val dependencies = Map[ModId, Set[ActorRef]]()

  val updatedMods = Set[ModId]()

  def read[T, U](mod: Mod[T], reader: T => (Changeable[U])): Changeable[U] = {
    val readNode = ddg.addRead(mod.asInstanceOf[Mod[Any]],
                               currentParent,
                               reader.asInstanceOf[Any => Changeable[Any]])

    val outerReader = currentParent
    currentParent = readNode

    val value =
      if (mods.contains(mod.id)) {
        mods(mod.id).asInstanceOf[T]
      } else {
        mod.read(workerRef)
      }

    val changeable = reader(value)
    currentParent = outerReader

    changeable
  }

  def write[T](dest: Dest[T], value: T): Changeable[T] = {
    awaiting += dest.mod.update(value, workerRef, this)

    if (ddg.reads.contains(dest.mod.id)) {
      ddg.modUpdated(dest.mod.id)
    }

    val changeable = new Changeable(dest.mod)
    //ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]], currentParent)

    changeable
  }

  def mod[T](initializer: Dest[T] => Changeable[T]): Mod[T] = {
    val d = new Dest[T](workerRef)
    dependencies(d.mod.id) = Set()
    initializer(d).mod
  }

  var workerId = 0
  def par[T, U](one: TBD => T, two: TBD => U): Tuple2[T, U] = {
    val task1 =  new Task(((tbd: TBD) => one(tbd)))
    val workerProps1 =
      Worker.props(id + "-" + workerId, datastoreRef, workerRef)
    val workerRef1 = system.actorOf(workerProps1, id + "-" + workerId)
    workerId += 1
    val oneFuture = workerRef1 ? RunTaskMessage(task1)

    val task2 =  new Task(((tbd: TBD) => two(tbd)))
    val workerProps2 =
      Worker.props(id + "-" + workerId, datastoreRef, workerRef)
    val workerRef2 = system.actorOf(workerProps2, id + "-" +workerId)
    workerId += 1
    val twoFuture = workerRef2 ? RunTaskMessage(task2)

    ddg.addPar(workerRef1, workerRef2, currentParent)

    val oneRet = Await.result(oneFuture, timeout.duration).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, timeout.duration).asInstanceOf[U]
    new Tuple2(oneRet, twoRet)
  }

  var memoId = 0
  val memoTable = Map[List[Any], MemoEntry]()
  def makeLift[T, U](): Lift[T, U] = {
    val thisMemoId = memoId
    memoId += 1
    new Lift((args: List[Mod[T]], func: () => Changeable[U]) => {
      val memoized =
        if (initialRun) {
          false
        } else {
	  var updated = false
	  for (arg <- args) {
	    if (updatedMods.contains(arg.id)) {
	      updated = true
	    }
	  }
          !updated
        }

      if (memoized && memoTable.contains(thisMemoId :: args)) {
	log.debug("Found memoized value for call to " + thisMemoId)
        val memoEntry = memoTable(thisMemoId :: args)
        ddg.attachSubtree(currentParent, memoEntry.node)
        memoEntry.changeable.asInstanceOf[Changeable[U]]
      } else {
	log.debug("Did not find memoized value for call to " + thisMemoId)

        val memoNode = ddg.addMemo(currentParent, (thisMemoId :: args))
        val outerParent = currentParent
        currentParent = memoNode
        val changeable = func()
        currentParent = outerParent

        val memoEntry = new MemoEntry(changeable.asInstanceOf[Changeable[Any]],
                                        memoNode)
        memoTable += ((thisMemoId :: args) -> memoEntry)

        changeable
      }
    })
  }

  def map[T, U](arr: Array[Mod[T]], func: T => U): Array[Mod[U]] = {
    arr.map((elem) =>
      mod((dest: Dest[U]) => read(elem, (value: T) => write(dest, func(value)))))
  }


}
