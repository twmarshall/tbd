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
import akka.actor.{ActorRef, Props}
import akka.event.{Logging, LoggingAdapter}
import akka.util.Timeout
import scala.concurrent.{Await, Promise}
import scala.concurrent.duration._

import tbd.ddg.{DDG, ReadId}
import tbd.manager.Manager
import tbd.input.Reader
import tbd.messages._
import tbd.mod.Mod
import tbd.worker.{InitialWorker, Task}

object TBD {
  var id = 0
}

trait TBD {
  var input: Reader = null
  private var ddgRef: ActorRef  = null
  private var manager: Manager = null
  private var log: LoggingAdapter = null

  private var currentReader = new ReadId()

  def initialize(aManager: Manager, aDDGRef: ActorRef) {
    manager = aManager
    ddgRef = aDDGRef
    log = Logging(manager.getSystem, "TBD")
    input = new Reader(manager)
  }

  def run(dest: Dest): Changeable[Any]

  def read[T](mod: Mod[T], reader: (T) => (Changeable[T])): Changeable[T] = {
    log.debug("Executing read on  mod " + mod.id)

    val readId = new ReadId()
    ddgRef ! AddReadMessage(mod.id, readId)

    if (currentReader.id != 0)
      ddgRef ! AddCallMessage(currentReader, readId)

    val outerReader = currentReader
    currentReader = readId
    val changeable = reader(mod.read())
    currentReader = outerReader

    ddgRef ! AddWriteMessage(readId, changeable.mod.id)

    implicit val timeout = Timeout(5 seconds)
    val toStringFuture = ddgRef ? ToStringMessage
    val result = Await.result(toStringFuture, timeout.duration).asInstanceOf[String]

    log.debug("Contents of DDG after read:\n" + result)

    changeable
  }

  def write[T](dest: Dest, value: T): Changeable[T] = {
    val mod = new Mod(value, manager)
    log.debug("Writing " + value + " to " + mod.id)
    new Changeable(mod)
  }

  def mod[T](initializer: Dest => Changeable[T]): Mod[T] = {
    initializer(new Dest()).mod
  }

  var i = 0
  def par[T, U](one: Dest => (Changeable[T]),
		two: Dest => (Changeable[U])): Tuple2[Mod[T], Mod[U]] = {
    i += 1
    val promiseTwo = Promise[T]()
    val ref = manager.getSystem.actorOf(Props(classOf[InitialWorker[T]], promiseTwo, i),
		                                    "workerActor" + i)
    ref ! RunTaskMessage(new Task((() => two(new Dest)).asInstanceOf[() => (Changeable[Any])]))

    val oneMod = one(new Dest()).mod

    implicit val timeout = Timeout(5 seconds)
    val twoMod = Await.result(promiseTwo.future, timeout.duration)
      .asInstanceOf[Mod[U]]
    new Tuple2(oneMod, twoMod)
  }

  def map[T](arr: Array[Mod[T]], func: (T) => (T)): Array[Mod[T]] = {
    arr.map((elem) =>
      mod((dest: Dest) => read(elem, (value: T) => write(dest, func(value)))))
  }

  def map[T](node: Mod[ListNode[T]], func: (T) => (T)): Mod[ListNode[T]] = {
    def innerMap(dest: Dest, lst: ListNode[T]): Changeable[ListNode[T]] = {
      if (lst != null) {
        val newValue = mod((dest) =>
          read(lst.value, (value: T) => write(dest, func(value))))
        val newNext = mod((dest) =>
          read(lst.next, (next: ListNode[T]) => innerMap(dest, next)))
        write(dest, new ListNode[T](newValue, newNext))
      } else {
        write(dest, null)
      }
    }
    mod((dest) => read(node, (lst: ListNode[T]) => innerMap(dest, lst)))
  }

  def parMap[T](node: Mod[ListNode[T]], func: (T) => (T)): Mod[ListNode[T]] = {
    def innerMap(dest: Dest, lst: ListNode[T]): Changeable[ListNode[T]] = {
      if (lst != null) {
	      val modTuple =
          par(valueDest => {
	          read(lst.value, (value: T) => write(valueDest, func(value)))
	        }, nextDest => {
	          read(lst.next, (next: ListNode[T]) => innerMap(nextDest, next))
	        })
        write(dest, new ListNode[T](modTuple._1, modTuple._2))
      } else {
        write(dest, null)
      }
    }
    mod((dest) => read(node, (lst: ListNode[T]) => innerMap(dest, lst)))
  }
}
