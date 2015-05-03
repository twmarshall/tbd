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
package tdb

import akka.actor.ActorRef
import akka.pattern.ask
import akka.util.Timeout
import java.io.Serializable
import java.util.concurrent.TimeUnit.MILLISECONDS
import scala.collection.mutable
import scala.concurrent.{Await, ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

import tdb.Constants._
import tdb.messages._

class Resolver(masterRef: ActorRef) extends Serializable {
  val tasks = mutable.Map[TaskId, ActorRef]()

  private final val TIME = 500000

  def resolve(taskId: TaskId): ActorRef = {
    if (!tasks.contains(taskId)) {
      val taskRef = Await.result(
        (masterRef ? ResolveMessage(taskId)).mapTo[ActorRef], DURATION)
      tasks(taskId) = taskRef
    }

    tasks(taskId)
  }

  def sendToTask(taskId: TaskId, message: Any, round: Int = 1)
      (onComplete: => Unit)
      (implicit ec: ExecutionContext) {
    val taskRef = resolve(taskId)
    val f = ask(taskRef, message)(Timeout(TIME * round, MILLISECONDS))

    f.onComplete {
      case Success(f) =>
        onComplete
      case Failure(e) =>
        println("Sending failed to " + taskRef)
        val newTaskRef = Await.result(
          (masterRef ? ResolveMessage(taskId)).mapTo[ActorRef], DURATION)
        println("Retrieved new taskRef = " + newTaskRef)
        tasks(taskId) = newTaskRef

        sendToTask(taskId, message, round + 1)(onComplete)
    }
  }

  def send(taskId: TaskId, message: Any)
      (implicit ec: ExecutionContext): Future[Any] = {
    val promise = Promise[Any]
    sendHelper(taskId, message, 1, promise)
    promise.future
  }

  private def sendHelper
      (taskId: TaskId, message: Any, round: Int, promise: Promise[Any])
      (implicit ec: ExecutionContext) {
    val taskRef = resolve(taskId)
    val f = ask(taskRef, message)(Timeout(TIME * round, MILLISECONDS))

    f.onComplete {
      case Success(v) =>
        promise.success(v)
      case Failure(e) =>
        println("Sending failed to " + taskRef)
        val newTaskRef = Await.result(
          (masterRef ? ResolveMessage(taskId)).mapTo[ActorRef], DURATION)
        println("Retrieved new taskRef = " + newTaskRef)

        val newMessage =
          if (newTaskRef == taskRef) {
            // This prevents us from resending messages that were actually
            // received but that we timed out on because the sender took too
            // long to respond (which happens a lot during recovery since if
            // the receiver has a bad ref it will have to wait for it to time
            // out). This could cause problems if the failure was actually the
            // result of a transient network error, but Akka should handle this
            // for us (hopefully).
            "ping"
          } else {
            message
          }
        tasks(taskId) = newTaskRef

        sendHelper(taskId, newMessage, round, promise)
    }
  }
}
