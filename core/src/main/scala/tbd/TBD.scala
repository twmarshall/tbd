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
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import tbd.ddg.{DDG, Node}
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
    system: ActorSystem,
    initialRun: Boolean) {

  var currentParent: Node = null
  val input = new Reader(datastoreRef)

  val log = Logging(system, "TBD" + id)

  implicit val timeout = Timeout(300 seconds)

  // Represents the number of PebblingFinishedMessages this worker is waiting
  // on before it can finish.
  var awaiting = 0

  def read[T, U](mod: Mod[T], reader: T => (Changeable[U])): Changeable[U] = {
    log.debug("Executing read on  mod " + mod.id)

    val readNode = ddg.addRead(mod.asInstanceOf[Mod[Any]], currentParent, reader)

    val outerReader = currentParent
    currentParent = readNode
    val changeable = reader(mod.read(workerRef))
    currentParent = outerReader

    changeable
  }

  def write[T](dest: Dest[T], value: T): Changeable[T] = {
    log.debug("Writing  to " + dest.mod.id)

    val future = datastoreRef ! UpdateModMessage(dest.mod.id, value, workerRef)
    if (ddg.reads.contains(dest.mod.id)) {
      ddg.modUpdated(dest.mod.id)
    }
    awaiting += 1

    val changeable = new Changeable(dest.mod)
    //ddg.addWrite(changeable.mod.asInstanceOf[Mod[Any]], currentParent)

    changeable
  }

  def mod[T](initializer: Dest[T] => Changeable[T]): Mod[T] = {
    initializer(new Dest(datastoreRef)).mod
  }

  var workerId = 0
  def par[T, U](one: TBD => T, two: TBD => U): Tuple2[T, U] = {
    log.debug("Executing par.")
    val task1 =  new Task(((tbd: TBD) => one(tbd)))
    val workerProps1 =
      Worker.props[U](id + "-" + workerId, datastoreRef, workerRef)
    val workerRef1 = system.actorOf(workerProps1, id + "-" + workerId)
    workerId += 1
    val oneFuture = workerRef1 ? RunTaskMessage(task1)

    val task2 =  new Task(((tbd: TBD) => two(tbd)))
    val workerProps2 =
      Worker.props[U](id + "-" + workerId, datastoreRef, workerRef)
    val workerRef2 = system.actorOf(workerProps2, id + "-" +workerId)
    workerId += 1
    val twoFuture = workerRef2 ? RunTaskMessage(task2)

    ddg.addPar(workerRef1, workerRef2, currentParent)

    val oneRet = Await.result(oneFuture, timeout.duration).asInstanceOf[T]
    val twoRet = Await.result(twoFuture, timeout.duration).asInstanceOf[U]
    new Tuple2(oneRet, twoRet)
  }

  /*var memoId = 0
  def memo[T, U](): (List[Mod[T]]) => (() => Changeable[U]) => Changeable[U] = {
    implicit val timeout = Timeout(5 seconds)
    val thisMemoId = memoId
    memoId += 1
    (args: List[Mod[T]]) => {
      (func: () => Changeable[U]) => {
	if (initialRun) {
	  runMemo(args, func, thisMemoId)
	} else {
	  val updatedFuture = datastoreRef ? GetUpdatedMessage
	  val updatedMods = Await.result(updatedFuture, timeout.duration)
	    .asInstanceOf[Set[ModId]]

	  var updated = false
	  for (arg <- args) {
	    if (updatedMods.contains(arg.id)) {
	      updated = true
	    }
	  }

	  if (updated) {
	    log.debug("Did not find memoized value for call to " + thisMemoId)
	    runMemo(args, func, thisMemoId)
	  } else {
	    val memoFuture = datastoreRef ?
	      GetMessage("memo", thisMemoId :: args)
	    val memo = Await.result(memoFuture, timeout.duration)
	      .asInstanceOf[Changeable[U]]
	    log.debug("Found memoized value for call to " + thisMemoId)
	    memo
	  }
	}
      }
    }
  }

  private def runMemo[T, U](
      args: List[Mod[T]],
      func: () => Changeable[U],
      thisMemoId: Int): Changeable[U] = {
    val changeable = func()
    datastoreRef ! PutMessage("memo", thisMemoId :: args, changeable)
    changeable
  }*/

  def map[T, U](arr: Array[Mod[T]], func: T => U): Array[Mod[U]] = {
    arr.map((elem) =>
      mod((dest: Dest[U]) => read(elem, (value: T) => write(dest, func(value)))))
  }


}
